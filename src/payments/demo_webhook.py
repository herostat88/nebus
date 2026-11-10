"""Local callback receiver for the one-command demonstration, not a production service."""

import hashlib
import hmac
import json
import os

from fastapi import FastAPI, Header, HTTPException, Request

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
events: dict[str, dict] = {}


@app.post("/hook")
async def receive(
    request: Request, x_webhook_id: str = Header(), x_webhook_signature: str = Header()
):
    body = await request.body()
    expected = (
        "sha256="
        + hmac.new(os.environ["WEBHOOK_SECRET"].encode(), body, hashlib.sha256).hexdigest()
    )
    if not hmac.compare_digest(expected.encode(), x_webhook_signature.encode()):
        raise HTTPException(401, "Invalid signature")
    # In production this deduplication must be persistent and transactional with side effects.
    events.setdefault(x_webhook_id, json.loads(body))
    return {"received": True}


@app.post("/fail")
async def fail():
    raise HTTPException(503, "Intentional failure for retry/DLQ demonstration")


@app.get("/events")
async def received(x_api_key: str = Header()):
    if not hmac.compare_digest(x_api_key.encode(), os.environ["API_KEY"].encode()):
        raise HTTPException(401, "Invalid API key")
    return list(events.values())
