"""The orders-schema history applies to a real PostgreSQL and enforces its constraints."""

from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from order_api.models import Customer, Item, SalesEntity


@pytest.mark.asyncio
async def test_orders_history_is_stamped_in_its_own_schema(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version_num FROM orders.alembic_version"))
    assert result.scalar_one()


@pytest.mark.asyncio
async def test_duplicate_sales_entity_code_is_rejected(
    db_session: AsyncSession,
) -> None:
    db_session.add(SalesEntity(code="E1", name="First"))
    await db_session.commit()

    db_session.add(SalesEntity(code="E1", name="Duplicate"))
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_duplicate_customer_code_within_entity_is_rejected(
    db_session: AsyncSession,
) -> None:
    entity = SalesEntity(code="E2", name="Entity Two")
    db_session.add(entity)
    await db_session.flush()

    db_session.add(Customer(entity_id=entity.id, code="C1", name="First"))
    await db_session.commit()

    db_session.add(Customer(entity_id=entity.id, code="C1", name="Duplicate"))
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_same_customer_code_is_allowed_across_entities(
    db_session: AsyncSession,
) -> None:
    entity_a = SalesEntity(code="E3", name="Entity Three")
    entity_b = SalesEntity(code="E4", name="Entity Four")
    db_session.add_all([entity_a, entity_b])
    await db_session.flush()

    db_session.add(Customer(entity_id=entity_a.id, code="C1", name="A's customer"))
    db_session.add(Customer(entity_id=entity_b.id, code="C1", name="B's customer"))
    await db_session.commit()  # raises nothing — this is the assertion


@pytest.mark.asyncio
async def test_duplicate_item_code_is_rejected(db_session: AsyncSession) -> None:
    db_session.add(Item(code="I1", name="First", list_price=Decimal("9.99")))
    await db_session.commit()

    db_session.add(Item(code="I1", name="Duplicate", list_price=Decimal("9.99")))
    with pytest.raises(IntegrityError):
        await db_session.commit()
