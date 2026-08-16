"""The inventory-schema history applies to a real PostgreSQL and enforces its constraints."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from inventory_worker.models import Inventory, InventoryReservation


@pytest.mark.asyncio
async def test_inventory_history_is_stamped_in_its_own_schema(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version_num FROM inventory.alembic_version"))
    assert result.scalar_one()


@pytest.mark.asyncio
async def test_new_inventory_row_starts_with_zero_reserved(
    db_session: AsyncSession,
) -> None:
    row = Inventory(entity_id=uuid.uuid4(), item_id=uuid.uuid4(), quantity_on_hand=100)
    db_session.add(row)
    await db_session.commit()
    await db_session.refresh(row)

    assert row.quantity_reserved == 0


@pytest.mark.asyncio
async def test_reservation_requires_a_matching_inventory_row(
    db_session: AsyncSession,
) -> None:
    reservation = InventoryReservation(
        entity_id=uuid.uuid4(),
        order_id=uuid.uuid4(),
        item_id=uuid.uuid4(),
        quantity=1,
        created_by_event=uuid.uuid4(),
    )
    db_session.add(reservation)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_invalid_reservation_status_is_rejected(db_session: AsyncSession) -> None:
    entity_id, item_id = uuid.uuid4(), uuid.uuid4()
    db_session.add(Inventory(entity_id=entity_id, item_id=item_id, quantity_on_hand=10))
    await db_session.flush()

    reservation = InventoryReservation(
        entity_id=entity_id,
        order_id=uuid.uuid4(),
        item_id=item_id,
        quantity=1,
        status="BOGUS",
        created_by_event=uuid.uuid4(),
    )
    db_session.add(reservation)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_duplicate_active_reservation_for_same_order_item_is_rejected(
    db_session: AsyncSession,
) -> None:
    entity_id, item_id, order_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db_session.add(Inventory(entity_id=entity_id, item_id=item_id, quantity_on_hand=10))
    await db_session.flush()

    def _reservation() -> InventoryReservation:
        return InventoryReservation(
            entity_id=entity_id,
            order_id=order_id,
            item_id=item_id,
            quantity=1,
            created_by_event=uuid.uuid4(),
        )

    db_session.add(_reservation())
    await db_session.commit()

    db_session.add(_reservation())
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_released_reservation_allows_a_new_active_one_for_the_same_order_item(
    db_session: AsyncSession,
) -> None:
    entity_id, item_id, order_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db_session.add(Inventory(entity_id=entity_id, item_id=item_id, quantity_on_hand=10))
    await db_session.flush()

    released = InventoryReservation(
        entity_id=entity_id,
        order_id=order_id,
        item_id=item_id,
        quantity=1,
        status="RELEASED",
        created_by_event=uuid.uuid4(),
    )
    db_session.add(released)
    await db_session.commit()

    fresh = InventoryReservation(
        entity_id=entity_id,
        order_id=order_id,
        item_id=item_id,
        quantity=1,
        created_by_event=uuid.uuid4(),
    )
    db_session.add(fresh)
    await db_session.commit()  # raises nothing — this is the assertion


@pytest.mark.asyncio
async def test_inventory_outbox_index_is_independent_of_orders_outbox_index(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = 'inventory' AND indexname = 'ix_outbox_unpublished'"
            )
        )
    indexdef = result.scalar_one()
    assert "published_at IS NULL" in indexdef
