"""A pooled connection the server has dropped must never reach the caller."""

import pytest
from sqlalchemy import URL, create_engine, text
from sqlalchemy.pool import NullPool

from common.db import make_sync_engine


@pytest.mark.asyncio
async def test_a_connection_the_server_dropped_is_replaced_before_use(
    migrated_database: URL,
) -> None:
    engine = make_sync_engine()
    with engine.connect() as connection:
        dropped: int = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
    # Kill it from outside the pool, the way a database restart would; a
    # pooled killer would be handed the very connection it is meant to kill.
    killer = create_engine(engine.url, poolclass=NullPool)
    with killer.connect() as connection:
        connection.execute(text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": dropped})

    with engine.connect() as connection:
        current: int = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
    engine.dispose()
    killer.dispose()

    assert current != dropped
