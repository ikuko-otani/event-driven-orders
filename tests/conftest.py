"""Fixtures: an ephemeral PostgreSQL, migrated by the real Alembic histories.

Tests run against a real PostgreSQL, never a mock or SQLite: schemas, partial
indexes and composite primary keys are precisely what a substitute cannot
verify, and they are what this schema's correctness rests on.
"""

import os
import uuid
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import suppress
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from confluent_kafka import Consumer

# NewTopic is re-exported without a declaration upstream, so mypy cannot see it
# as part of confluent_kafka.admin's public surface; the path itself is the
# documented one.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
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
    os.environ["REDIS_PORT"] = str(
        redis_container.get_exposed_port(redis_container.port)
    )
    return RedisSettings().url


def _start_redpanda(attempts: int = 3) -> RedpandaContainer:
    """Start the broker, retrying a start race inside testcontainers itself.

    The module pushes the start script into an already-running container whose
    shell waits on `[ -f script ]` — existence, not content — so under host load
    the file can be seen while still empty, run as a no-op, and the container
    exits 0 with no broker in it. Retrying is the only lever available here.
    """
    while True:
        attempts -= 1
        container = RedpandaContainer("redpandadata/redpanda:v26.1.14")
        try:
            return container.start(timeout=30)
        except RuntimeError:
            with suppress(Exception):
                container.stop()
            if attempts == 0:
                raise


@pytest.fixture(scope="session")
def redpanda_container() -> Generator[RedpandaContainer, None, None]:
    """One Redpanda broker for the whole test session, on the image compose runs."""
    container = _start_redpanda()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def configured_kafka(redpanda_container: RedpandaContainer) -> str:
    """Point KAFKA_* at the container, the same way migrated_database points DB_*."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = redpanda_container.get_bootstrap_server()
    return KafkaSettings().bootstrap_servers


@pytest.fixture
def make_topic(configured_kafka: str) -> Generator[Callable[[str], str], None, None]:
    """Create topics by name for one test, and delete every one of them after it.

    Auto-creation is off (design §6.2), so a topic nobody creates is a send
    that fails — never a 1-partition topic appearing silently. The name is an
    argument rather than a constant because the dead-letter path needs a
    second topic whose name ConsumerConfig derives, not this fixture.
    """
    admin = AdminClient({"bootstrap.servers": configured_kafka})
    created: list[str] = []

    # Waiting on the future is what makes the topic exist before the test
    # publishes to it; create_topics only queues the request.
    def make(name: str) -> str:
        for future in admin.create_topics([NewTopic(name, num_partitions=1)]).values():
            future.result()
        created.append(name)
        return name

    yield make

    # A leftover topic would let one test's messages answer the next test's
    # "was anything published?", so every topic goes away with its test.
    if created:
        for future in admin.delete_topics(created).values():
            future.result()


@pytest.fixture
def kafka_topic(make_topic: Callable[[str], str]) -> str:
    """The single source topic most tests need, named so it cannot collide."""
    return make_topic(f"orders.events.{uuid.uuid4()}")


@pytest.fixture
def kafka_consumer(configured_kafka: str) -> Generator[Consumer, None, None]:
    """A consumer in a group of its own, reading its topic from the beginning.

    The group id is where Kafka remembers how far a consumer has read, and
    earliest is what keeps a message published before the subscription
    visible — which is the order every test here runs in (§7.3).
    """
    consumer = Consumer(
        {
            "bootstrap.servers": configured_kafka,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    yield consumer
    consumer.close()


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
def sync_engine(migrated_database: str) -> Generator[Engine, None, None]:
    """A synchronous engine per test: the poller and the consumers are sync processes."""
    engine = create_engine(DatabaseSettings().sync_url)
    yield engine
    engine.dispose()


@pytest.fixture
def sync_session(sync_engine: Engine) -> Generator[Session, None, None]:
    """One synchronous Session, reaching the container over its own connection."""
    with Session(sync_engine) as session:
        yield session


@pytest.fixture
def sync_session_factory(sync_engine: Engine) -> Callable[[], Session]:
    """What a consuming process is given: a way to open one session per message."""
    return sessionmaker(sync_engine)
