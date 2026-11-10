import asyncio
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from payments.gateway import SimulatedGateway
from payments.models import Payment
from payments.webhooks import WebhookClient, WebhookError, signature


async def test_webhook_payload_signature_and_id_are_stable_across_retries(
    factory, settings, payment
):
    requests = []

    async def respond(request):
        requests.append(request)
        return httpx.Response(503 if len(requests) == 1 else 204)

    gateway = AsyncMock()
    gateway.charge.return_value = "succeeded"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        webhook = WebhookClient(settings, client)
        async with factory.begin() as session:
            stored = await session.get(Payment, payment.id)
            from payments.processing import utcnow

            stored.status = "succeeded"
            stored.processed_at = utcnow()
        async with factory() as session:
            stored = await session.get(Payment, payment.id)
            with pytest.raises(WebhookError):
                await webhook.send(stored)
            await webhook.send(stored)
    assert requests[0].content == requests[1].content
    assert requests[0].headers["X-Webhook-Id"] == str(payment.id)
    assert requests[0].headers["X-Webhook-Signature"] == signature(
        requests[0].content, settings.webhook_secret.get_secret_value()
    )
    assert json.loads(requests[0].content)["amount"] == "125.50"


async def test_redirect_is_not_followed(factory, settings, payment):
    async with factory.begin() as session:
        stored = await session.get(Payment, payment.id)
        from payments.processing import utcnow

        stored.status, stored.processed_at = "failed", utcnow()
    requests = []

    def redirect(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect)) as client:
        with pytest.raises(WebhookError):
            await WebhookClient(settings, client).send(stored)
    assert len(requests) == 1


async def test_gateway_is_repeatable_and_has_success_and_decline_outcomes(settings):
    gateway = SimulatedGateway(settings)
    target = uuid4()
    assert await gateway.charge(target) == await gateway.charge(target)
    settings.gateway_success_rate = 0
    assert await gateway.charge(target) == "failed"
    settings.gateway_success_rate = 1
    assert await gateway.charge(target) == "succeeded"


async def test_webhook_has_total_deadline_even_for_trickling_response(settings, payment):
    from payments.processing import utcnow

    class Trickle(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(100):
                await asyncio.sleep(0.01)
                yield b"x"

    payment.status, payment.processed_at = "succeeded", utcnow()
    settings.webhook_timeout = 0.04
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Trickle()))
    ) as client:
        async with asyncio.timeout(0.5):
            with pytest.raises(WebhookError, match="deadline"):
                await WebhookClient(settings, client).send(payment)


async def test_webhook_response_size_is_bounded(settings, payment):
    from payments.processing import utcnow

    payment.status, payment.processed_at = "succeeded", utcnow()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=b"x" * (settings.webhook_max_response_bytes + 1))
        )
    ) as client:
        with pytest.raises(WebhookError, match="too_large"):
            await WebhookClient(settings, client).send(payment)
