"""Database connection settings, read from the environment."""

from pydantic_settings import BaseSettings, SettingsConfigDict


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
    def sync_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
        )

    @property
    def async_url(self) -> str:
        return self.sync_url.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)


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
