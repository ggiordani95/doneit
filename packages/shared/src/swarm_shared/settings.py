"""Configuration.

Every process reads the same settings object. Values come from environment
variables, optionally seeded by a local ``.env``; secrets are never defaulted
to a working value, so a missing credential fails loudly at startup rather
than silently at the first API call.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "BrokerSettings",
    "DatabaseSettings",
    "RedisSettings",
    "Settings",
    "WorkerSettings",
    "get_settings",
]

_ENV_FILE = ".env"


class _Base(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


class BrokerSettings(_Base):
    """Redpanda / Kafka-protocol connection.

    The names mirror the Kafka client configuration so that pointing the
    platform at Apache Kafka, MSK or Confluent Cloud is purely a matter of
    environment variables.
    """

    bootstrap_servers: str = Field(
        default="localhost:19092",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = Field(
        default="PLAINTEXT",
        validation_alias="KAFKA_SECURITY_PROTOCOL",
    )
    sasl_mechanism: str | None = Field(default=None, validation_alias="KAFKA_SASL_MECHANISM")
    sasl_username: str | None = Field(default=None, validation_alias="KAFKA_SASL_USERNAME")
    sasl_password: SecretStr | None = Field(default=None, validation_alias="KAFKA_SASL_PASSWORD")

    client_id: str = Field(default="swarm", validation_alias="KAFKA_CLIENT_ID")
    request_timeout_ms: int = Field(default=40_000, validation_alias="KAFKA_REQUEST_TIMEOUT_MS")

    @property
    def servers(self) -> list[str]:
        return [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]


class DatabaseSettings(_Base):
    url: str = Field(
        default="postgresql+asyncpg://swarm:swarm@localhost:5452/swarm",
        validation_alias="DATABASE_URL",
    )
    pool_size: int = Field(default=10, validation_alias="DATABASE_POOL_SIZE")
    max_overflow: int = Field(default=5, validation_alias="DATABASE_MAX_OVERFLOW")
    echo: bool = Field(default=False, validation_alias="DATABASE_ECHO")

    @field_validator("url")
    @classmethod
    def _require_async_driver(cls, v: str) -> str:
        if v.startswith("postgresql://"):
            # A sync driver here would block the event loop under load.
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    @property
    def sync_url(self) -> str:
        """Alembic runs migrations synchronously."""
        return self.url.replace("+asyncpg", "")


class RedisSettings(_Base):
    url: str = Field(default="redis://localhost:6381/0", validation_alias="REDIS_URL")


class WorkerSettings(_Base):
    agent_type: str = Field(default="researcher", validation_alias="AGENT_TYPE")
    concurrency: int = Field(default=4, ge=1, le=64, validation_alias="WORKER_CONCURRENCY")
    task_timeout_s: float = Field(default=600.0, gt=0, validation_alias="TASK_TIMEOUT_S")
    shutdown_grace_s: float = Field(default=60.0, gt=0, validation_alias="SHUTDOWN_GRACE_S")

    max_attempts: int = Field(default=4, ge=1, validation_alias="RETRY_MAX_ATTEMPTS")
    initial_delay_s: float = Field(default=1.0, gt=0, validation_alias="RETRY_INITIAL_DELAY_S")
    max_delay_s: float = Field(default=60.0, gt=0, validation_alias="RETRY_MAX_DELAY_S")
    backoff_multiplier: float = Field(default=2.0, ge=1, validation_alias="RETRY_BACKOFF")

    @property
    def group_id(self) -> str:
        return f"agent-{self.agent_type}"

    @property
    def max_poll_interval_ms(self) -> int:
        """Poll interval must outlast the slowest task, as a safety net.

        Execution is decoupled from the poll loop, so this should never be
        reached; it exists so that a worker whose loop does stall is evicted
        rather than silently holding partitions.
        """
        return int(self.task_timeout_s * 1000) + 60_000


class Settings(_Base):
    """Root settings object. Build it once per process via ``get_settings``."""

    env: Literal["local", "ci", "staging", "production"] = Field(
        default="local", validation_alias="SWARM_ENV"
    )
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    log_format: Literal["console", "json"] = Field(default="console", validation_alias="LOG_FORMAT")
    workspaces_dir: Path = Field(default=Path("./workspaces"), validation_alias="SWARM_WORKSPACES")

    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)

    @property
    def is_production(self) -> bool:
        return self.env == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings, read once.

    Cached so that importing modules can call it freely. Tests that need a
    different configuration call ``get_settings.cache_clear()``.
    """
    return Settings()
