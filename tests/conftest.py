import os
import subprocess
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete
from sqlalchemy.engine import make_url

from payments.api import create_app
from payments.config import Settings
from payments.db import create_engine, sessions
from payments.models import Outbox, Payment
from payments.schemas import PaymentCreate
from payments.service import create_payment


@pytest.fixture(scope="session", autouse=True)
def migrated_database():
    url = os.environ["DATABASE_URL"]
    if not (make_url(url).database or "").endswith("_test"):
        pytest.fail("Tests refuse to run against a database not ending in _test")
    subprocess.run(["alembic", "upgrade", "head"], check=True)


@pytest.fixture
def settings():
    return Settings(rabbitmq_prefix="test-" + uuid4().hex)


@pytest.fixture
async def factory(settings):
    engine = create_engine(settings)
    session_factory = sessions(engine)
    async with session_factory.begin() as session:
        await session.execute(delete(Outbox))
        await session.execute(delete(Payment))
    yield session_factory
    await engine.dispose()


@pytest.fixture
def payload():
    return {
        "amount": "125.50",
        "currency": "RUB",
        "description": "Test order",
        "metadata": {"order_id": "synthetic-order-001"},
        "webhook_url": "http://webhook:8080/hook",
    }


@pytest.fixture
async def payment(factory, payload):
    return await create_payment(
        factory, PaymentCreate.model_validate(payload), "test-" + uuid4().hex
    )


@pytest.fixture
async def client(settings, factory):
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://api",
        headers={"X-API-Key": settings.api_key.get_secret_value()},
    ) as client:
        yield client
    await app.state.engine.dispose()
