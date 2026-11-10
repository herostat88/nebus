import asyncio
import logging
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from faststream import AckPolicy
from faststream.rabbit import (
    Channel,
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)
from pydantic import ValidationError

from payments.config import Settings
from payments.models import Outbox
from payments.processing import InvalidEvent, PaymentProcessor
from payments.schemas import PaymentEvent

logger = logging.getLogger(__name__)


class Messaging:
    def __init__(self, settings: Settings):
        self.settings = settings
        prefix = settings.rabbitmq_prefix
        self.exchange = RabbitExchange(prefix, type=ExchangeType.DIRECT, durable=True)
        self.dlx = RabbitExchange(prefix + ".dlx", type=ExchangeType.DIRECT, durable=True)
        self.queue = RabbitQueue(
            prefix + ".new",
            durable=True,
            routing_key=prefix + ".new",
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": self.dlx.name,
                "x-dead-letter-routing-key": prefix + ".dead",
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
                "x-delivery-limit": settings.broker_delivery_limit,
                "x-max-length": settings.broker_queue_max_length,
                "x-single-active-consumer": True,
            },
        )
        self.dlq = RabbitQueue(
            prefix + ".dlq",
            durable=True,
            routing_key=prefix + ".dead",
            arguments={
                "x-queue-type": "quorum",
                "x-overflow": "reject-publish",
                "x-max-length": settings.broker_queue_max_length,
            },
        )
        url = urlsplit(settings.rabbitmq_url.get_secret_value())
        query = dict(parse_qsl(url.query)) | {"heartbeat": str(settings.broker_heartbeat)}
        broker_url = urlunsplit(url._replace(query=urlencode(query)))
        self.broker = RabbitBroker(
            broker_url,
            default_channel=Channel(
                prefetch_count=1, publisher_confirms=True, on_return_raises=True
            ),
            graceful_timeout=settings.shutdown_timeout,
        )

    async def declare(self):
        await self.broker.connect()
        exchange = await self.broker.declare_exchange(self.exchange)
        dlx = await self.broker.declare_exchange(self.dlx)
        dlq = await self.broker.declare_queue(self.dlq)
        await dlq.bind(dlx, routing_key=self.dlq.routing_key)
        queue = await self.broker.declare_queue(self.queue)
        await queue.bind(exchange, routing_key=self.queue.routing_key)

    async def publish(self, event: Outbox):
        dead = event.routing_key == "payments.dead"
        if event.routing_key not in {"payments.new", "payments.dead"}:
            raise ValueError("Unknown outbox routing key")
        result = await self.broker.publish(
            event.payload,
            exchange=self.dlx if dead else self.exchange,
            routing_key=self.dlq.routing_key if dead else self.queue.routing_key,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=self.settings.publish_timeout,
        )
        if result is False:
            raise RuntimeError("Broker rejected publication")

    def subscribe(self, processor: PaymentProcessor):
        @self.broker.subscriber(self.queue, self.exchange, ack_policy=AckPolicy.MANUAL)
        async def consume(message: RabbitMessage):
            try:
                event = PaymentEvent.model_validate_json(message.body)
            except (ValidationError, ValueError):
                logger.warning("invalid_payment_message")
                await message.reject(requeue=False)
                return
            try:
                await processor.process(event)
            except InvalidEvent:
                logger.warning("invalid_payment_event payment_id=%s", event.payment_id)
                await message.reject(requeue=False)
            except Exception:
                logger.exception("payment_infrastructure_error payment_id=%s", event.payment_id)
                await asyncio.sleep(self.settings.infra_retry_delay)
                await message.nack(requeue=True)
            else:
                await message.ack()

        return consume
