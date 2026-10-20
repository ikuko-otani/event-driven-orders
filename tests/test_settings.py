"""A database password must reach the driver exactly as it was given."""

from common.settings import DatabaseSettings


def test_password_with_url_special_characters_survives_unchanged() -> None:
    password = "p%40ss:w#rd?x"
    settings = DatabaseSettings(password=password)

    sync_url = settings.sync_url
    async_url = settings.async_url

    assert sync_url.password == password
    assert async_url.password == password
    assert async_url.drivername == "postgresql+asyncpg"
