"""Fixtures: an ephemeral PostgreSQL, migrated by the real Alembic histories.

Tests run against a real PostgreSQL, never a mock or SQLite: schemas, partial
indexes and composite primary keys are precisely what a substitute cannot
verify, and they are what this schema's correctness rests on.
"""

import os
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.postgres import PostgresContainer

from common.settings import DatabaseSettings

REPO_ROOT = Path(__file__).resolve().parent.parent

# One Alembic history per owning service (design §3.1). Both are applied here,
# exactly as an operator applies them locally.
ALEMBIC_INIS = [REPO_ROOT / "services" / "order-api" / "alembic.ini"]


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """One PostgreSQL container for the whole test session."""
    with PostgresContainer("postgres:18.4-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def migrated_database(postgres_container: PostgresContainer) -> str:
    """Point DB_* at the container, run every migration once, return the async URL.

    The container's coordinates are published as environment variables rather
    than passed as arguments, so Alembic reaches the test database through the
    same `common.settings` path production uses.
    """
    os.environ["DB_HOST"] = postgres_container.get_container_host_ip()
    os.environ["DB_PORT"] = str(postgres_container.get_exposed_port(5432))
    os.environ["DB_NAME"] = postgres_container.dbname
    os.environ["DB_USER"] = postgres_container.username
    os.environ["DB_PASSWORD"] = postgres_container.password

    for ini in ALEMBIC_INIS:
        command.upgrade(Config(str(ini)), "head")

    return DatabaseSettings().async_url


@pytest_asyncio.fixture
async def engine(migrated_database: str) -> AsyncGenerator[AsyncEngine, None]:
    """A fresh async engine per test, so no connection state leaks between tests."""
    engine = create_async_engine(migrated_database)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Empty every domain table before each test.

    The table list is read from the database instead of hard-coded, so it stays
    correct as migrations add tables. alembic_version is excluded: truncating it
    would tell Alembic the database is unmigrated.
    """
    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "SELECT format('%I.%I', schemaname, tablename) FROM pg_tables "
                "WHERE schemaname IN ('orders', 'inventory') "
                "AND tablename <> 'alembic_version'"
            )
        )
        tables = [row[0] for row in result]
        if tables:
            await conn.execute(text(f"TRUNCATE TABLE {', '.join(tables)} CASCADE"))
    yield


@pytest_asyncio.fixture
async def db_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """One AsyncSession per test."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
