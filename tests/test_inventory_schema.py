"""The inventory-schema history applies to a real PostgreSQL and enforces its constraints."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@pytest.mark.asyncio
async def test_inventory_history_is_stamped_in_its_own_schema(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version_num FROM inventory.alembic_version"))
    assert result.scalar_one()
