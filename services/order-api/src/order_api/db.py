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
    """One session per request; the session's transaction is the request's transaction."""
    sessionmaker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with sessionmaker() as session:
        yield session
        await session.commit()
