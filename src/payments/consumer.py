import asyncio
import logging
import signal
from pathlib import Path

import httpx

from payments.broker import Messaging
from payments.config import Settings
from payments.db import create_engine, sessions
from payments.gateway import SimulatedGateway
from payments.outbox import OutboxPublisher
from payments.processing import PaymentProcessor
from payments.webhooks import WebhookClient


def configure_logging(settings: Settings) -> None:
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    # Dependency request logs include full callback URLs, potentially with query tokens.
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).disabled = True


async def main():
    ready = Path("/tmp/consumer-ready")
    await asyncio.to_thread(ready.unlink, missing_ok=True)
    settings = Settings()
    configure_logging(settings)
    engine = create_engine(settings)
    messaging = Messaging(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as client:
        processor = PaymentProcessor(
            sessions(engine), settings, SimulatedGateway(settings), WebhookClient(settings, client)
        )
        messaging.subscribe(processor)
        try:
            await messaging.declare()
            await messaging.broker.start()
            await asyncio.to_thread(ready.touch)
            publisher = OutboxPublisher(sessions(engine), settings, messaging.publish)
            task = asyncio.create_task(publisher.run(stop), name="outbox-publisher")
            try:
                await stop.wait()
            finally:
                stop.set()
                await asyncio.wait_for(task, timeout=settings.shutdown_timeout)
        finally:
            await asyncio.to_thread(ready.unlink, missing_ok=True)
            await messaging.broker.stop()
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
