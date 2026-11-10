import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from payments.broker import Messaging
from payments.models import Outbox, Payment
from payments.outbox import OutboxPublisher
from payments.processing import PaymentProcessor
from payments.webhooks import WebhookError


@pytest.fixture
async def messaging(settings):
    messaging = Messaging(settings)
    await messaging.declare()
    yield messaging
    await messaging.broker.stop()


async def wait_until(check):
    async with asyncio.timeout(10):
        # The condition lives in PostgreSQL/RabbitMQ, not an in-process Event.
        while not (result := await check()):  # noqa: ASYNC110
            await asyncio.sleep(0.02)
        return result


async def test_real_broker_persistent_publish_and_outbox_confirmation(
    factory, settings, payment, messaging
):
    await OutboxPublisher(factory, settings, messaging.publish).publish_one()
    queue = await messaging.broker.declare_queue(messaging.queue)
    # Quorum single-active-consumer queues require basic.consume, not basic.get.
    async with queue.iterator() as iterator:
        message = await asyncio.wait_for(anext(iterator), timeout=5)
        payload = json.loads(message.body)
        assert payload["payment_id"] == str(payment.id)
        assert message.delivery_mode == 2
        assert message.message_id == payload["event_id"]
        await message.ack()
    async with factory() as session:
        assert (await session.scalar(select(Outbox))).published_at


async def test_unroutable_publication_is_not_marked_published(
    factory, settings, payment, messaging
):
    queue = await messaging.broker.declare_queue(messaging.queue)
    exchange = await messaging.broker.declare_exchange(messaging.exchange)
    await queue.unbind(exchange, routing_key=messaging.queue.routing_key)
    await OutboxPublisher(factory, settings, messaging.publish).publish_one()
    async with factory() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None and event.publish_attempts == 1


async def test_real_consumer_processes_payment_then_acknowledges(
    factory, settings, payment, messaging
):
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    messaging.subscribe(PaymentProcessor(factory, settings, gateway, webhooks))
    await messaging.broker.start()
    await OutboxPublisher(factory, settings, messaging.publish).publish_one()

    async def delivered():
        async with factory() as session:
            return (await session.get(Payment, payment.id)).webhook_delivered_at

    await wait_until(delivered)
    gateway.charge.assert_awaited_once()
    webhooks.send.assert_awaited_once()


async def test_real_consumer_retry_pipeline_ends_in_dlq(factory, settings, payment, messaging):
    settings.retry_base_seconds = 0.05
    settings.outbox_poll_interval = 0.02
    gateway, webhooks = AsyncMock(), AsyncMock()
    gateway.charge.return_value = "succeeded"
    webhooks.send.side_effect = WebhookError("synthetic callback failure")
    messaging.subscribe(PaymentProcessor(factory, settings, gateway, webhooks))
    await messaging.broker.start()
    stop = asyncio.Event()
    task = asyncio.create_task(OutboxPublisher(factory, settings, messaging.publish).run(stop))
    dlq = await messaging.broker.declare_queue(messaging.dlq)
    try:

        async def get_message():
            return await dlq.get(fail=False, timeout=1)

        message = await wait_until(get_message)
        body = json.loads(message.body)
        assert body["event_type"] == "payments.dead" and body["attempt"] == 3
        assert body["payment_id"] == str(payment.id)
        assert webhooks.send.await_count == 3
        gateway.charge.assert_awaited_once()
        await message.ack()
    finally:
        stop.set()
        await task


async def test_poison_payload_is_rejected_into_native_dlq(factory, settings, messaging):
    processor = PaymentProcessor(factory, settings, AsyncMock(), AsyncMock())
    messaging.subscribe(processor)
    await messaging.broker.start()
    await messaging.broker.publish(
        {"invalid": "synthetic poison message"},
        exchange=messaging.exchange,
        routing_key=messaging.queue.routing_key,
        persist=True,
    )
    dlq = await messaging.broker.declare_queue(messaging.dlq)

    async def get_message():
        return await dlq.get(fail=False, timeout=1)

    message = await wait_until(get_message)
    assert json.loads(message.body) == {"invalid": "synthetic poison message"}
    assert message.headers["x-death"]
    await message.ack()


async def test_infrastructure_outage_does_not_exhaust_twenty_redeliveries(
    factory, settings, payment, messaging
):
    settings.infra_retry_delay = 0.01
    recovered = asyncio.Event()
    calls = 0

    async def process(_):
        nonlocal calls
        calls += 1
        if calls <= 25:
            raise ConnectionError("synthetic database outage")
        recovered.set()

    processor = AsyncMock()
    processor.process.side_effect = process
    messaging.subscribe(processor)
    await messaging.broker.start()
    await OutboxPublisher(factory, settings, messaging.publish).publish_one()
    await asyncio.wait_for(recovered.wait(), timeout=10)
    assert calls == 26
