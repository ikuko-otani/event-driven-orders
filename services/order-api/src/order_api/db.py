"""order-api's async engine and per-request session dependency."""

from collections.abc import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from common.settings import DatabaseSettings


def make_engine() -> AsyncEngine:
    """Build the engine once, from the environment (`common.settings`)."""
    return create_async_engine(DatabaseSettings().async_url)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    """One session per request; the route commits, this guarantees the rollback.

    A yield dependency's cleanup runs after FastAPI has sent the response, so a
    commit here could not change what the client was already told. Ending a
    failed transaction is the part that is still meaningful afterwards.
    """
    sessionmaker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with sessionmaker() as session:
        try:
            yield session
        except Exception:
            # A route that raised may have flushed rows already, so end its
            # transaction here rather than leaving it to the connection's return.
            await session.rollback()
            raise
