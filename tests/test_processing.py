import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from payments.models import Outbox, Payment
from payments.processing import InvalidEvent, PaymentProcessor, utcnow
from payments.schemas import PaymentEvent
from payments.webhooks import WebhookError


async def first_event(factory):
    async with factory() as session:
        return PaymentEvent.model_validate((await session.scalar(select(Outbox))).payload)


async def test_success_and_repeated_delivery_charge_and_notify_once(factory, settings, payment):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    processor = PaymentProcessor(factory, settings, gateway, webhooks)
    event = await first_event(factory)
    await processor.process(event)
    await processor.process(event)
    gateway.charge.assert_awaited_once_with(payment.id)
    webhooks.send.assert_awaited_once()
    async with factory() as session:
        stored = await session.get(Payment, payment.id)
        assert stored.status == "succeeded"
        assert stored.processed_at and stored.webhook_delivered_at
        assert stored.attempts == 1


async def test_concurrent_duplicate_deliveries_are_serialized(factory, settings, payment):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    processor = PaymentProcessor(factory, settings, gateway, webhooks)
    event = await first_event(factory)
    await asyncio.gather(processor.process(event), processor.process(event))
    gateway.charge.assert_awaited_once()
    webhooks.send.assert_awaited_once()


async def test_declined_payment_is_a_business_result_and_not_retried(factory, settings, payment):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "failed"
    await PaymentProcessor(factory, settings, gateway, webhooks).process(await first_event(factory))
    assert webhooks.send.call_args.args[0].status == "failed"
    async with factory() as session:
        assert len((await session.scalars(select(Outbox))).all()) == 1
        assert (await session.get(Payment, payment.id)).status == "failed"


async def test_retries_are_persistent_exponential_and_dead_lettered_after_three(
    factory, settings, payment
):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks.send.side_effect = WebhookError("unavailable")
    processor = PaymentProcessor(factory, settings, gateway, webhooks)
    event = await first_event(factory)
    for attempt in range(1, 4):
        before = utcnow()
        # A new processor models restart between attempts: no counters kept in memory.
        processor = PaymentProcessor(factory, settings, gateway, webhooks)
        await processor.process(event)
        async with factory() as session:
            stored = await session.get(Payment, payment.id)
            assert stored.attempts == attempt
            assert stored.status == "succeeded"
            assert stored.webhook_delivered_at is None
            rows = (await session.scalars(select(Outbox).order_by(Outbox.created_at))).all()
            next_row = rows[-1]
            if attempt < 3:
                assert next_row.routing_key == "payments.new"
                assert before + timedelta(seconds=2 ** (attempt - 1)) <= next_row.available_at
                event = PaymentEvent.model_validate(next_row.payload)
            else:
                assert next_row.routing_key == "payments.dead"
                assert next_row.payload["attempt"] == 3
                assert stored.dead_lettered_at
    await processor.process(event)
    assert webhooks.send.await_count == 3
    gateway.charge.assert_awaited_once()


async def test_webhook_retry_can_succeed_without_charging_again(factory, settings, payment):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks.send.side_effect = [WebhookError("timeout"), None]
    processor = PaymentProcessor(factory, settings, gateway, webhooks)
    await processor.process(await first_event(factory))
    async with factory() as session:
        retry = await session.scalar(
            select(Outbox).where(Outbox.payload["attempt"].as_integer() == 2)
        )
    await processor.process(PaymentEvent.model_validate(retry.payload))
    gateway.charge.assert_awaited_once()
    assert webhooks.send.await_count == 2
    async with factory() as session:
        stored = await session.get(Payment, payment.id)
        assert stored.webhook_delivered_at and stored.last_error is None
        assert stored.attempts == 2


async def test_gateway_result_is_committed_before_callback(factory, settings, payment):
    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"

    async def check_visibility(_):
        async with factory() as session:
            stored = await session.get(Payment, payment.id)
            assert stored.status == "succeeded" and stored.processed_at

    webhooks = AsyncMock()
    webhooks.send.side_effect = check_visibility
    await PaymentProcessor(factory, settings, gateway, webhooks).process(await first_event(factory))


async def test_cancellation_after_gateway_commit_does_not_recharge(factory, settings, payment):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks.send.side_effect = asyncio.CancelledError
    processor = PaymentProcessor(factory, settings, gateway, webhooks)
    event = await first_event(factory)
    with pytest.raises(asyncio.CancelledError):
        await processor.process(event)
    webhooks.send.side_effect = None
    await PaymentProcessor(factory, settings, gateway, webhooks).process(event)
    gateway.charge.assert_awaited_once()
    async with factory() as session:
        assert (await session.get(Payment, payment.id)).webhook_delivered_at


async def test_unknown_payment_and_out_of_order_attempt_are_invalid(factory, settings, payment):
    processor = PaymentProcessor(factory, settings, AsyncMock(), AsyncMock())
    for target, attempt in ((uuid4(), 1), (payment.id, 2)):
        with pytest.raises(InvalidEvent):
            await processor.process(
                PaymentEvent(event_id=uuid4(), payment_id=target, attempt=attempt)
            )
