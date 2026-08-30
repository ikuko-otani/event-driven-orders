"""Fixtures: an ephemeral PostgreSQL, migrated by the real Alembic histories.

Tests run against a real PostgreSQL, never a mock or SQLite: schemas, partial
indexes and composite primary keys are precisely what a substitute cannot
verify, and they are what this schema's correctness rests on.
"""

import os
import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config

# NewTopic is re-exported without a declaration upstream, so mypy cannot see it
# as part of confluent_kafka.admin's public surface; the path itself is the
# documented one.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session
from testcontainers.community.kafka import RedpandaContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from common.settings import DatabaseSettings, KafkaSettings, RedisSettings
from order_api.main import app

REPO_ROOT = Path(__file__).resolve().parent.parent

# One Alembic history per owning service (design §3.1). Both are applied here,
# exactly as an operator applies them locally.
ALEMBIC_INIS = [
    REPO_ROOT / "services" / "order-api" / "alembic.ini",
    REPO_ROOT / "services" / "inventory-worker" / "alembic.ini",
]


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


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer, None, None]:
    """One Redis container for the whole test session."""
    with RedisContainer("redis:8.10.1-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def configured_redis(redis_container: RedisContainer) -> str:
    """Point REDIS_* at the container, the same way migrated_database points DB_*."""
    os.environ["REDIS_HOST"] = redis_container.get_container_host_ip()
    os.environ["REDIS_PORT"] = str(redis_container.get_exposed_port(redis_container.port))
    return RedisSettings().url


@pytest.fixture(scope="session")
def redpanda_container() -> Generator[RedpandaContainer, None, None]:
    """One Redpanda broker for the whole test session, on the image compose runs."""
    with RedpandaContainer("redpandadata/redpanda:v26.1.14") as redpanda:
        yield redpanda


@pytest.fixture(scope="session")
def configured_kafka(redpanda_container: RedpandaContainer) -> str:
    """Point KAFKA_* at the container, the same way migrated_database points DB_*."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = redpanda_container.get_bootstrap_server()
    return KafkaSettings().bootstrap_servers


@pytest.fixture
def kafka_topic(configured_kafka: str) -> Generator[str, None, None]:
    """A topic of this test's own, created explicitly and deleted afterwards.

    Auto-creation is off (design §6.2), so a topic nobody creates is a send
    that fails — never a 1-partition topic appearing silently. A topic shared
    between tests would also let one test's leftovers answer the next test's
    "was anything published?".
    """
    admin = AdminClient({"bootstrap.servers": configured_kafka})
    topic = f"orders.events.{uuid.uuid4()}"
    for future in admin.create_topics([NewTopic(topic, num_partitions=1)]).values():
        future.result()
    yield topic
    for future in admin.delete_topics([topic]).values():
        future.result()


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
async def redis_client(configured_redis: str) -> AsyncGenerator[Redis, None]:
    """One Redis client per test."""
    client = Redis.from_url(configured_redis)
    yield client
    await client.aclose()


@pytest_asyncio.fixture(autouse=True)
async def clean_redis(redis_client: Redis) -> AsyncGenerator[None, None]:
    """Empty the cache before each test, mirroring clean_db for Postgres."""
    await redis_client.flushdb()
    yield


@pytest_asyncio.fixture
async def db_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """One AsyncSession per test."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest_asyncio.fixture
async def api_client(
    migrated_database: str, configured_redis: str
) -> AsyncGenerator[AsyncClient, None]:
    """An HTTP client wired to the real app, with the app's own lifespan run around it."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
def sync_session(migrated_database: str) -> Generator[Session, None, None]:
    """A synchronous Session, because the poller is a synchronous process.

    It reaches the same container over its own connection, exactly as the
    poller reaches the database in production — never sharing the app's.
    """
    engine = create_engine(DatabaseSettings().sync_url)
    with Session(engine) as session:
        yield session
    engine.dispose()
