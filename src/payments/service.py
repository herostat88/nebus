import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.models import Outbox, Payment
from payments.schemas import PaymentCreate, PaymentEvent


class IdempotencyConflict(Exception):
    pass


def new_event(payment_id: UUID, attempt: int) -> Outbox:
    event_id = uuid4()
    return Outbox(
        id=event_id,
        routing_key="payments.new",
        payload=PaymentEvent(event_id=event_id, payment_id=payment_id, attempt=attempt).model_dump(
            mode="json"
        ),
    )


async def create_payment(factory: async_sessionmaker, request: PaymentCreate, key: str) -> Payment:
    payload = request.canonical_payload()
    request_hash = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()
    async with factory.begin() as session:
        payment_id = await session.scalar(
            insert(Payment)
            .values(
                id=uuid4(),
                amount=request.amount,
                currency=request.currency.value,
                description=request.description,
                metadata_json=request.metadata,
                webhook_url=str(request.webhook_url),
                idempotency_key=key,
                request_hash=request_hash,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        if payment_id is None:
            payment = await session.scalar(select(Payment).where(Payment.idempotency_key == key))
            if payment.request_hash != request_hash:
                raise IdempotencyConflict
            return payment
        session.add(new_event(payment_id, 1))
        return await session.get(Payment, payment_id)
