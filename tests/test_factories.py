"""Prove the factory helpers in factories.py compose and persist correctly."""

import pytest
from factories import (
    create_customer,
    create_inventory,
    create_item,
    create_sales_entity,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.models import Customer, SalesEntity


@pytest.mark.asyncio
async def test_customer_factory_links_to_its_entity_and_is_visible_before_commit(
    db_session: AsyncSession,
) -> None:
    entity = await create_sales_entity(db_session, code="E-FAC", name="Factory Test Entity")
    customer = await create_customer(
        db_session, entity=entity, code="C-FAC", name="Factory Test Customer"
    )

    assert customer.entity_id == entity.id

    result = await db_session.execute(select(Customer).where(Customer.id == customer.id))
    assert result.scalar_one().entity_id == entity.id

    result = await db_session.execute(select(SalesEntity).where(SalesEntity.id == entity.id))
    assert result.scalar_one().code == "E-FAC"


@pytest.mark.asyncio
async def test_inventory_factory_shares_entity_and_item_ids_from_its_overrides(
    db_session: AsyncSession,
) -> None:
    entity = await create_sales_entity(db_session)
    item = await create_item(db_session, code="I-FAC")

    inventory = await create_inventory(db_session, entity=entity, item=item, quantity_on_hand=42)

    assert inventory.entity_id == entity.id
    assert inventory.item_id == item.id
    assert inventory.quantity_on_hand == 42
