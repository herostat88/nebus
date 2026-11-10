import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text

from payments.config import Settings
from payments.db import create_engine, sessions
from payments.models import Payment
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentDetail
from payments.service import IdempotencyConflict, create_payment
from payments.webhooks import validate_webhook_url


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = create_engine(settings)
    factory = sessions(engine)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await engine.dispose()

    async def authenticate(x_api_key: Annotated[str | None, Header()] = None):
        if x_api_key is None or not secrets.compare_digest(
            x_api_key.encode(), settings.api_key.get_secret_value().encode()
        ):
            raise HTTPException(401, "Invalid API key")

    app = FastAPI(
        title="Async payment processor",
        version="1.0.0",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.engine = engine
    app.state.sessions = factory

    @app.get("/health", include_in_schema=False)
    async def health():
        async with factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi():
        return get_openapi(title=app.title, version=app.version, routes=app.routes)

    @app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
    async def create(
        body: PaymentCreate,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[!-~]+$")],
    ):
        try:
            validate_webhook_url(str(body.webhook_url), settings)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        try:
            payment = await create_payment(factory, body, idempotency_key)
        except IdempotencyConflict as error:
            raise HTTPException(409, "Idempotency key was used with a different request") from error
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
    async def get(payment_id: UUID):
        async with factory() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise HTTPException(404, "Payment not found")
            return PaymentDetail(
                payment_id=payment.id,
                status=payment.status,
                created_at=payment.created_at,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata_json,
                webhook_url=payment.webhook_url,
                processed_at=payment.processed_at,
                attempts=payment.attempts,
                webhook_delivered_at=payment.webhook_delivered_at,
                dead_lettered_at=payment.dead_lettered_at,
                last_error=payment.last_error,
            )

    return app
