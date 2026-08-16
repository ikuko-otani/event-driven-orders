"""The orders-schema history applies to a real PostgreSQL and enforces its constraints."""

import uuid
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from order_api.models import Customer, Item, Order, OrderLine, Outbox, SalesEntity


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


@pytest.mark.asyncio
async def test_duplicate_order_number_within_entity_is_rejected(
    db_session: AsyncSession,
) -> None:
    entity = SalesEntity(code="E5", name="Entity Five")
    db_session.add(entity)
    await db_session.flush()
    customer = Customer(entity_id=entity.id, code="C1", name="A customer")
    db_session.add(customer)
    await db_session.flush()

    def _order(order_number: str) -> Order:
        return Order(
            entity_id=entity.id,
            customer_id=customer.id,
            order_number=order_number,
            idempotency_key=f"key-{order_number}",
            currency="EUR",
            delivery_date=date(2026, 9, 1),
        )

    db_session.add(_order("ORD-1"))
    await db_session.commit()

    db_session.add(_order("ORD-1"))
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_within_entity_is_rejected(
    db_session: AsyncSession,
) -> None:
    entity = SalesEntity(code="E6", name="Entity Six")
    db_session.add(entity)
    await db_session.flush()
    customer = Customer(entity_id=entity.id, code="C1", name="A customer")
    db_session.add(customer)
    await db_session.flush()

    def _order(order_number: str, idempotency_key: str) -> Order:
        return Order(
            entity_id=entity.id,
            customer_id=customer.id,
            order_number=order_number,
            idempotency_key=idempotency_key,
            currency="EUR",
            delivery_date=date(2026, 9, 1),
        )

    db_session.add(_order("ORD-2", "same-key"))
    await db_session.commit()

    db_session.add(_order("ORD-3", "same-key"))
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_invalid_order_status_is_rejected(db_session: AsyncSession) -> None:
    entity = SalesEntity(code="E7", name="Entity Seven")
    db_session.add(entity)
    await db_session.flush()
    customer = Customer(entity_id=entity.id, code="C1", name="A customer")
    db_session.add(customer)
    await db_session.flush()

    order = Order(
        entity_id=entity.id,
        customer_id=customer.id,
        order_number="ORD-4",
        status="BOGUS",
        idempotency_key="key-4",
        currency="EUR",
        delivery_date=date(2026, 9, 1),
    )
    db_session.add(order)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_duplicate_order_line_item_is_rejected(db_session: AsyncSession) -> None:
    entity = SalesEntity(code="E8", name="Entity Eight")
    db_session.add(entity)
    await db_session.flush()
    customer = Customer(entity_id=entity.id, code="C1", name="A customer")
    item = Item(code="I2", name="Widget", list_price=Decimal("5.00"))
    db_session.add_all([customer, item])
    await db_session.flush()

    order = Order(
        entity_id=entity.id,
        customer_id=customer.id,
        order_number="ORD-5",
        idempotency_key="key-5",
        currency="EUR",
        delivery_date=date(2026, 9, 1),
    )
    db_session.add(order)
    await db_session.flush()

    db_session.add(
        OrderLine(order_id=order.id, item_id=item.id, quantity=1, unit_price=Decimal("5.00"))
    )
    await db_session.commit()

    db_session.add(
        OrderLine(order_id=order.id, item_id=item.id, quantity=2, unit_price=Decimal("5.00"))
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_outbox_poller_index_only_covers_healthy_unpublished_rows(
    engine: AsyncEngine,
) -> None:
    async with engine.connect() as conn:
        result = await conn.execute(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE schemaname = 'orders' AND indexname = 'ix_outbox_unpublished'"
            )
        )
    indexdef = result.scalar_one()
    assert "published_at IS NULL" in indexdef
    assert "quarantined_at IS NULL" in indexdef


@pytest.mark.asyncio
async def test_new_outbox_row_starts_unpublished_at_version_one(
    db_session: AsyncSession,
) -> None:
    row = Outbox(
        entity_id=uuid.uuid4(),
        aggregate_type="Order",
        aggregate_id=uuid.uuid4(),
        event_type="OrderConfirmed",
        payload={"order_id": "9a1f"},
    )
    db_session.add(row)
    await db_session.commit()
    await db_session.refresh(row)

    assert row.event_version == 1
    assert row.publish_attempts == 0
    assert row.published_at is None
    assert row.quarantined_at is None
    assert row.created_at is not None
