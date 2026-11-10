import asyncio
import hashlib
import hmac
import json
from urllib.parse import urlsplit

import httpx

from payments.config import Settings
from payments.models import Payment


class WebhookError(Exception):
    pass


def validate_webhook_url(url: str, settings: Settings) -> None:
    parsed = urlsplit(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.username
        or parsed.password
        or parsed.fragment
        or (parsed.hostname or "").lower().rstrip(".") not in settings.allowed_hosts
        or port not in settings.allowed_ports
    ):
        raise ValueError("Webhook host/port is not allowed")


def webhook_body(payment: Payment) -> bytes:
    return json.dumps(
        {
            "event_id": str(payment.id),
            "event_type": "payment.processed",
            "payment_id": str(payment.id),
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "status": payment.status,
            "description": payment.description,
            "metadata": payment.metadata_json,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def signature(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient):
        self.settings = settings
        self.client = client

    async def send(self, payment: Payment) -> None:
        validate_webhook_url(payment.webhook_url, self.settings)
        body = webhook_body(payment)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(payment.id),
            "X-Webhook-Signature": signature(body, self.settings.webhook_secret.get_secret_value()),
        }
        try:
            async with asyncio.timeout(self.settings.webhook_timeout):
                async with self.client.stream(
                    "POST",
                    payment.webhook_url,
                    content=body,
                    headers=headers,
                    timeout=self.settings.webhook_timeout,
                    follow_redirects=False,
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise WebhookError(f"webhook_http_{response.status_code}")
                    # A trickle response must not stall the only worker indefinitely.
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.settings.webhook_max_response_bytes:
                            raise WebhookError("webhook_response_too_large")
        except TimeoutError as error:
            raise WebhookError("webhook_deadline_exceeded") from error
        except httpx.HTTPError as error:
            raise WebhookError("webhook_transport_error") from error
