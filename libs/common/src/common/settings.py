"""Database connection settings, read from the environment."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class DatabaseSettings(BaseSettings):
    """One source for the connection URL, rendered in the two forms needed.

    Alembic runs synchronously (psycopg) and the services run asynchronously
    (asyncpg); deriving both from the same fields keeps them from drifting.
    """

    model_config = SettingsConfigDict(env_prefix="DB_")

    host: str = "localhost"
    port: int = 5432
    name: str = "event_driven_orders"
    user: str = "app"
    password: str = "app"

    @property
    def sync_url(self) -> URL:
        # Each part stays a separate field, so a generated password containing
        # characters such as % # : never has to survive being parsed out of a string.
        return URL.create(
            "postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
        )

    @property
    def async_url(self) -> URL:
        # Same fields, another driver: the two forms cannot drift apart.
        return self.sync_url.set(drivername="postgresql+asyncpg")


class RedisSettings(BaseSettings):
    """Redis connection settings for the idempotency response cache (design §4.4)."""

    model_config = SettingsConfigDict(env_prefix="REDIS_")

    host: str = "localhost"
    port: int = 6379

    @property
    def url(self) -> str:
        return f"redis://{self.host}:{self.port}/0"


class KafkaSettings(BaseSettings):
    """Producer connection settings; the poller's delivery guarantee rests on them (design §5.4).

    message_timeout_ms bounds how long one send may stay unresolved: after it,
    the client reports the message as failed instead of retrying forever, which
    is what lets the poller's own attempt counting make progress at all.
    """

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:19092"
    message_timeout_ms: int = 300_000


class TracingSettings(BaseSettings):
    """Where this process sends its spans, if anywhere (design §2).

    The endpoint is optional on purpose: a process started without one still
    creates spans, so trace_id keeps reaching the log lines, but nothing is
    sent. That is what keeps a test run, or a process started by hand, from
    retrying deliveries to a collector that is not running.
    """

    model_config = SettingsConfigDict(env_prefix="OTEL_")

    exporter_otlp_endpoint: str | None = None
