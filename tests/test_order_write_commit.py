"""What a client is told when the transaction behind its answer fails to commit."""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from factories import make_customer, make_item, make_sales_entity
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.main import app
from order_api.models import Order

REFUSE_COMMIT = (
    "CREATE OR REPLACE FUNCTION orders.refuse_commit() RETURNS trigger "
    "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'commit refused'; END $$"
)
ARM_TRIGGER = (
    "CREATE CONSTRAINT TRIGGER refuse_commit AFTER INSERT ON orders.orders "
    "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION orders.refuse_commit()"
)


@pytest_asyncio.fixture
async def refuse_commit(db_session: AsyncSession) -> AsyncGenerator[None, None]:
    """Fail the COMMIT of an order insert while every statement before it succeeds.

    A DEFERRABLE INITIALLY DEFERRED constraint trigger fires at commit time
    rather than at the INSERT, which is the shape of a database that dies
    between the flush and the commit.
    """
    await db_session.execute(text(REFUSE_COMMIT))
    await db_session.execute(text(ARM_TRIGGER))
    await db_session.commit()

    yield

    # The trigger is DDL, so the table-truncating fixture cannot take it away.
    await db_session.execute(text("DROP TRIGGER IF EXISTS refuse_commit ON orders.orders"))
    await db_session.execute(text("DROP FUNCTION IF EXISTS orders.refuse_commit()"))
    await db_session.commit()


@pytest_asyncio.fixture
async def unguarded_client(
    migrated_database: str, configured_redis: str
) -> AsyncGenerator[AsyncClient, None]:
    """A client that turns an unhandled exception into a 500, as a real one would.

    The shared api_client re-raises into the test instead, which would hide the
    status code the caller is actually given — the subject of this test.
    """
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.mark.asyncio
async def test_a_refused_commit_is_reported_and_leaves_nothing_behind(
    db_session: AsyncSession,
    unguarded_client: AsyncClient,
    redis_client: Redis,
    refuse_commit: None,
) -> None:
    entity = await make_sales_entity(db_session)
    customer = await make_customer(db_session, entity=entity)
    item = await make_item(db_session)
    await db_session.commit()

    headers = {"X-Entity-Id": str(entity.id), "Idempotency-Key": "refused-commit-key"}
    payload = {
        "customer_id": str(customer.id),
        "currency": "JPY",
        "delivery_date": "2026-09-01",
        "lines": [{"item_id": str(item.id), "quantity": 1}],
    }

    # The commit fails, so the client must be told the create did not happen.
    refused = await unguarded_client.post("/orders", headers=headers, json=payload)

    assert refused.status_code == 500
    assert await db_session.scalar(select(func.count()).select_from(Order)) == 0
    assert await redis_client.get(f"idempotency:{entity.id}:refused-commit-key") is None

    # With the fault lifted the same key must still be free: an attempt that
    # cached its answer before committing would hand back a phantom order here.
    await db_session.execute(text("DROP TRIGGER refuse_commit ON orders.orders"))
    await db_session.commit()

    retried = await unguarded_client.post("/orders", headers=headers, json=payload)

    assert retried.status_code == 201
    assert await db_session.scalar(select(func.count()).select_from(Order)) == 1
