import logging
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.config import Settings
from payments.models import Outbox, Payment
from payments.schemas import PaymentEvent
from payments.service import new_event

logger = logging.getLogger(__name__)


class InvalidEvent(Exception):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


class PaymentProcessor:
    def __init__(self, factory: async_sessionmaker, settings: Settings, gateway, webhooks):
        self.factory = factory
        self.settings = settings
        self.gateway = gateway
        self.webhooks = webhooks

    async def locked_payment(self, session, event: PaymentEvent) -> Payment | None:
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise InvalidEvent("unknown_payment")
        if (
            payment.webhook_delivered_at
            or payment.dead_lettered_at
            or event.attempt <= payment.attempts
        ):
            return None
        if event.attempt != payment.attempts + 1 or event.attempt > self.settings.max_attempts:
            raise InvalidEvent("unexpected_attempt")
        return payment

    async def process(self, event: PaymentEvent) -> None:
        try:
            async with self.factory.begin() as session:
                payment = await self.locked_payment(session, event)
                if payment is None:
                    return
                if payment.status == "pending":
                    payment.status = await self.gateway.charge(payment.id)
                    if payment.status not in {"succeeded", "failed"}:
                        raise ValueError("invalid_gateway_result")
                    payment.processed_at = utcnow()
            # Commit the gateway result BEFORE notifying a client that may query the API.
            async with self.factory.begin() as session:
                payment = await self.locked_payment(session, event)
                if payment is None:
                    return
                await self.webhooks.send(payment)
                payment.attempts = event.attempt
                payment.webhook_delivered_at = utcnow()
                payment.last_error = None
            logger.info(
                "payment_complete payment_id=%s attempt=%s", event.payment_id, event.attempt
            )
        except InvalidEvent:
            raise
        except Exception as error:
            # Failure state and the next delivery (retry or DLQ) are committed atomically.
            await self.record_failure(event, type(error).__name__)

    async def record_failure(self, event: PaymentEvent, error_code: str) -> None:
        async with self.factory.begin() as session:
            payment = await self.locked_payment(session, event)
            if payment is None:
                return
            payment.attempts = event.attempt
            payment.last_error = error_code
            if event.attempt < self.settings.max_attempts:
                retry = new_event(payment.id, event.attempt + 1)
                retry.available_at = utcnow() + timedelta(
                    seconds=self.settings.retry_base_seconds * 2 ** (event.attempt - 1)
                )
                session.add(retry)
            else:
                payment.dead_lettered_at = utcnow()
                event_id = uuid4()
                session.add(
                    Outbox(
                        id=event_id,
                        routing_key="payments.dead",
                        payload={
                            "event_id": str(event_id),
                            "event_type": "payments.dead",
                            "payment_id": str(payment.id),
                            "attempt": event.attempt,
                            "original_event": event.model_dump(mode="json"),
                            "error": error_code,
                        },
                    )
                )
        logger.warning(
            "payment_retry_or_dlq payment_id=%s attempt=%s", event.payment_id, event.attempt
        )
