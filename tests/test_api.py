import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from payments.models import Outbox, Payment


async def test_create_and_get_payment(client, factory, payload):
    response = await client.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "order-1"}
    )
    assert response.status_code == 202, response.text
    accepted = response.json()
    assert accepted["status"] == "pending"
    detail = await client.get("/api/v1/payments/" + accepted["payment_id"])
    assert detail.status_code == 200
    assert Decimal(detail.json()["amount"]) == Decimal("125.50")
    assert detail.json()["metadata"] == payload["metadata"]
    assert detail.json()["processed_at"] is None
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        outbox = await session.scalar(select(Outbox))
        assert outbox.payload["payment_id"] == accepted["payment_id"]
        assert outbox.published_at is None


async def test_concurrent_idempotency_creates_only_one_payment_and_event(client, factory, payload):
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "same-key"})
            for _ in range(12)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_conflicting_idempotency_key_returns_409(client, factory, payload):
    headers = {"Idempotency-Key": "same-key"}
    assert (await client.post("/api/v1/payments", json=payload, headers=headers)).status_code == 202
    response = await client.post(
        "/api/v1/payments", json=payload | {"amount": "200.00"}, headers=headers
    )
    assert response.status_code == 409
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_equivalent_amount_and_json_key_order_are_idempotent(client, payload):
    headers = {"Idempotency-Key": "canonical"}
    first = await client.post(
        "/api/v1/payments",
        json=payload | {"amount": "125.5", "metadata": {"a": 1, "b": 2}},
        headers=headers,
    )
    second = await client.post(
        "/api/v1/payments", json=payload | {"metadata": {"b": 2, "a": 1}}, headers=headers
    )
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.parametrize("path", ["/health", "/openapi.json", "/api/v1/payments/" + str(uuid4())])
async def test_all_api_endpoints_require_key(client, path):
    assert (await client.get(path, headers={"X-API-Key": "wrong"})).status_code == 401


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"amount": "99999999999999999.00"},
        {"currency": "BTC"},
        {"metadata": {"large": "x" * 17000}},
        {"unexpected": "field"},
        {"webhook_url": "http://169.254.169.254/latest/meta-data/"},
        {"webhook_url": "http://webhook:5432/"},
        {"webhook_url": "file:///etc/passwd"},
        {"webhook_url": "http://user:password@webhook:8080/hook"},
        {"webhook_url": "http://webhook:8080/hook#fragment"},
    ],
)
async def test_invalid_payload_does_not_create_payment_or_event(client, factory, payload, change):
    response = await client.post(
        "/api/v1/payments", json=payload | change, headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422, response.text
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_required_headers_and_missing_payment(client, payload):
    assert (await client.post("/api/v1/payments", json=payload)).status_code == 422
    assert (
        await client.post(
            "/api/v1/payments", json=payload, headers={"Idempotency-Key": "space key"}
        )
    ).status_code == 422
    assert (await client.get("/api/v1/payments/" + str(uuid4()))).status_code == 404
    assert (await client.get("/openapi.json")).status_code == 200
