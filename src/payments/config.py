from typing import Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", allow_inf_nan=False, hide_input_in_errors=True
    )

    database_url: str
    rabbitmq_url: SecretStr
    api_key: SecretStr = Field(min_length=16)
    webhook_secret: SecretStr = Field(min_length=32)
    webhook_allowed_hosts: str = "webhook"
    webhook_allowed_ports: str = "80,443,8080"
    webhook_timeout: float = Field(default=5, gt=0, le=60)
    webhook_max_response_bytes: int = Field(default=65536, ge=1024)
    gateway_delay_min: float = Field(default=2, ge=0)
    gateway_delay_max: float = Field(default=5, ge=0, le=60)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    max_attempts: int = Field(default=3, ge=1, le=20)
    retry_base_seconds: float = Field(default=1, gt=0, le=60)
    outbox_poll_interval: float = Field(default=0.5, gt=0, le=60)
    outbox_batch_size: int = Field(default=20, ge=1, le=1000)
    publish_timeout: float = Field(default=5, gt=0, le=60)
    outbox_retry_max_seconds: float = Field(default=60, ge=1)
    infra_retry_delay: float = Field(default=1, gt=0, le=60)
    rabbitmq_prefix: str = "payments"
    broker_heartbeat: int = Field(default=30, ge=5)
    broker_delivery_limit: int = Field(default=1000, ge=1, le=2147483647)
    broker_queue_max_length: int = Field(default=10000, ge=1)
    shutdown_timeout: float = Field(default=15, gt=0)
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=5, ge=0)
    log_level: str = "INFO"

    @field_validator("database_url")
    @classmethod
    def postgres_only(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        return value

    @field_validator("rabbitmq_prefix")
    @classmethod
    def valid_prefix(cls, value: str) -> str:
        import re

        if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", value):
            raise ValueError("Invalid RabbitMQ prefix")
        return value

    @model_validator(mode="after")
    def validate_ranges(self) -> Self:
        if self.gateway_delay_max < self.gateway_delay_min:
            raise ValueError("Gateway maximum delay must be >= minimum delay")
        if not self.allowed_hosts or "*" in self.allowed_hosts:
            raise ValueError("Explicit WEBHOOK_ALLOWED_HOSTS are required; wildcards are forbidden")
        if not self.allowed_ports or any(not 1 <= port <= 65535 for port in self.allowed_ports):
            raise ValueError("Invalid WEBHOOK_ALLOWED_PORTS")
        return self

    @property
    def allowed_hosts(self) -> set[str]:
        return {
            host.strip().lower().rstrip(".")
            for host in self.webhook_allowed_hosts.split(",")
            if host.strip()
        }

    @property
    def allowed_ports(self) -> set[int]:
        return {int(port.strip()) for port in self.webhook_allowed_ports.split(",") if port.strip()}
