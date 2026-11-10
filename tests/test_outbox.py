import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from payments.models import Outbox, Payment
from payments.outbox import OutboxPublisher
from payments.processing import utcnow
from payments.schemas import PaymentCreate
from payments.service import create_payment


async def test_successful_publish_marks_event_only_after_confirmation(factory, settings, payment):
    publish = AsyncMock()
    dispatcher = OutboxPublisher(factory, settings, publish)
    assert await dispatcher.publish_one()
    assert not await dispatcher.publish_one()
    publish.assert_awaited_once()
    async with factory() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at and event.publish_attempts == 1


async def test_broker_failure_retains_event_and_payment(factory, settings, payment):
    dispatcher = OutboxPublisher(factory, settings, AsyncMock(side_effect=ConnectionError("down")))
    assert await dispatcher.publish_one()
    async with factory.begin() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None and event.publish_attempts == 1
        assert event.available_at > utcnow()
        assert await session.get(Payment, payment.id)
        event.available_at = utcnow() - timedelta(seconds=1)
    dispatcher.publish = AsyncMock()
    await dispatcher.publish_one()
    async with factory() as session:
        assert (await session.scalar(select(Outbox))).published_at


async def test_two_dispatchers_do_not_publish_the_same_locked_row(factory, settings, payment):
    started, finish = asyncio.Event(), asyncio.Event()

    async def publish(_):
        started.set()
        await finish.wait()

    first = asyncio.create_task(OutboxPublisher(factory, settings, publish).publish_one())
    await asyncio.wait_for(started.wait(), 5)
    second = OutboxPublisher(factory, settings, AsyncMock())
    assert not await second.publish_one()
    finish.set()
    await first
    second.publish.assert_not_called()


async def test_crash_after_broker_confirm_before_db_commit_republishes_same_event(
    factory, settings, payment
):
    ids = []

    async def confirmed_then_crashed(event):
        ids.append(event.id)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await OutboxPublisher(factory, settings, confirmed_then_crashed).publish_one()

    async def publish_again(event):
        ids.append(event.id)

    await OutboxPublisher(factory, settings, publish_again).publish_one()
    assert len(ids) == 2 and ids[0] == ids[1]


async def test_payment_and_outbox_are_rolled_back_together(factory, payload, monkeypatch):
    from payments import service

    def broken_event(*_):
        raise RuntimeError("synthetic failure before commit")

    monkeypatch.setattr(service, "new_event", broken_event)
    with pytest.raises(RuntimeError):
        await create_payment(factory, PaymentCreate.model_validate(payload), "rolled-back")
    async with factory() as session:
        assert (await session.scalars(select(Payment))).all() == []
        assert (await session.scalars(select(Outbox))).all() == []
