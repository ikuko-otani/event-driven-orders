"""The orders-schema history applies to a real PostgreSQL and enforces its constraints."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from order_api.models import SalesEntity


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
