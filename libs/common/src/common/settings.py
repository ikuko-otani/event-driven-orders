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
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def async_url(self) -> str:
        return self.sync_url.replace(
            "postgresql+psycopg://", "postgresql+asyncpg://", 1
        )
