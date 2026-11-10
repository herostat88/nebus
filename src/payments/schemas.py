import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("metadata")
    @classmethod
    def bounded_json(cls, value: dict) -> dict:
        if len(json.dumps(value, allow_nan=False).encode()) > 16384:
            raise ValueError("Metadata must not exceed 16 KiB")
        return value

    @field_validator("webhook_url")
    @classmethod
    def no_url_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError("Webhook credentials and fragments are not allowed")
        return value

    def canonical_payload(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        value["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None
    attempts: int
    webhook_delivered_at: datetime | None
    dead_lettered_at: datetime | None
    last_error: str | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    payment_id: UUID
    attempt: int = Field(ge=1)
    event_type: Literal["payments.new"] = "payments.new"
