import asyncio
import logging
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.config import Settings
from payments.models import Outbox
from payments.processing import utcnow

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self, factory: async_sessionmaker, settings: Settings, publish):
        self.factory = factory
        self.settings = settings
        self.publish = publish

    async def publish_one(self) -> bool:
        async with self.factory.begin() as session:
            event = await session.scalar(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                .order_by(Outbox.available_at, Outbox.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if event is None:
                return False
            try:
                await self.publish(event)
            except Exception as error:
                event.publish_attempts += 1
                event.last_error = type(error).__name__
                delay = min(
                    self.settings.retry_base_seconds * 2 ** min(event.publish_attempts - 1, 20),
                    self.settings.outbox_retry_max_seconds,
                )
                event.available_at = utcnow() + timedelta(seconds=delay)
                logger.warning("outbox_publish_failed event_id=%s", event.id)
            else:
                event.publish_attempts += 1
                event.published_at = utcnow()
                event.last_error = None
        return True

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                for _ in range(self.settings.outbox_batch_size):
                    if stop.is_set() or not await self.publish_one():
                        break
            except Exception:
                logger.exception("outbox_poll_failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.settings.outbox_poll_interval)
            except TimeoutError:
                pass
