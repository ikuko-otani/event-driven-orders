"""Factories for domain state that satisfies design §1's reproducibility rule.

Each helper takes the session and any overrides, adds the row, and returns the
ORM instance with its primary key populated after a flush — never committed
here, so callers control the transaction boundary.
"""

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.envelope import envelope
from inventory_worker.handlers import handle_order_confirmed
from inventory_worker.models import Inventory, Outbox
from order_api.events import order_confirmed_outbox
from order_api.models import Customer, Item, Order, OrderLine, SalesEntity
from order_api.services.orders import next_order_number


async def make_sales_entity(
    session: AsyncSession,
    *,
    code: str = "ENT-01",
    name: str = "Example Manufacturing Co.",
) -> SalesEntity:
    entity = SalesEntity(code=code, name=name)
    session.add(entity)
    await session.flush()
    return entity


async def make_customer(
    session: AsyncSession,
    *,
    entity: SalesEntity,
    code: str = "CUST-01",
    name: str = "Example Customer",
) -> Customer:
    customer = Customer(entity_id=entity.id, code=code, name=name)
    session.add(customer)
    await session.flush()
    return customer


async def make_item(
    session: AsyncSession,
    *,
    code: str = "ITEM-01",
    name: str = "Example Item",
    list_price: Decimal = Decimal("1000.00"),
) -> Item:
    item = Item(code=code, name=name, list_price=list_price)
    session.add(item)
    await session.flush()
    return item


async def make_inventory(
    session: AsyncSession,
    *,
    entity: SalesEntity,
    item: Item,
    quantity_on_hand: int = 100,
) -> Inventory:
    inventory = Inventory(entity_id=entity.id, item_id=item.id, quantity_on_hand=quantity_on_hand)
    session.add(inventory)
    await session.flush()
    return inventory


async def make_order(
    session: AsyncSession,
    *,
    entity: SalesEntity,
    customer: Customer,
    lines: Sequence[tuple[Item, int]],
    status: str = "PENDING",
    currency: str = "JPY",
    delivery_date: date = date(2026, 9, 1),
) -> Order:
    """Build an order in any status, numbered exactly as the API numbers it."""
    order = Order(
        entity_id=entity.id,
        customer_id=customer.id,
        order_number=await next_order_number(session),
        status=status,
        idempotency_key=f"factory-{uuid4()}",
        request_fingerprint="0" * 64,
        currency=currency,
        delivery_date=delivery_date,
    )
    order.lines = [
        OrderLine(item_id=item.id, quantity=quantity, unit_price=item.list_price)
        for item, quantity in lines
    ]
    session.add(order)
    await session.flush()
    return order


async def make_confirmed_order_event(
    session: AsyncSession, *, lines: Sequence[tuple[int, int]]
) -> dict[str, Any]:
    """Seed stock and a CONFIRMED order, and return the event its confirm published.

    Each pair is one line: the stock that exists, and the quantity ordered.
    """
    # One item, one inventory row and one order line per pair, so a test states
    # only the numbers that decide the outcome it is about.
    entity = await make_sales_entity(session)
    customer = await make_customer(session, entity=entity)
    ordered: list[tuple[Item, int]] = []
    for index, (on_hand, quantity) in enumerate(lines):
        item = await make_item(session, code=f"ITEM-{index:02d}")
        await make_inventory(session, entity=entity, item=item, quantity_on_hand=on_hand)
        ordered.append((item, quantity))

    # Build the event from a real outbox row, never by hand: a consumer test
    # must read exactly the bytes the poller would publish (design §4.1).
    order = await make_order(
        session, entity=entity, customer=customer, lines=ordered, status="CONFIRMED"
    )
    row = order_confirmed_outbox(order)
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return envelope(row)


def make_redelivery(event: dict[str, Any]) -> dict[str, Any]:
    """The same event carrying a fresh event_id (design §5.7).

    Two confirms racing each other write two outbox rows with distinct ids and
    an identical payload — the one duplicate processed_events cannot absorb,
    because it deduplicates on exactly the field that differs.
    """
    return {**event, "event_id": str(uuid4())}


async def make_inventory_reserved_event(
    session: AsyncSession,
    sync_session: Session,
    *,
    lines: Sequence[tuple[int, int]] = ((100, 3),),
) -> dict[str, Any]:
    """Replay confirm → reserve, and return the InventoryReserved that came back.

    The event is produced by running the real reservation handler rather than
    assembled by hand: order-api's consumer must be tested against the bytes
    the other service actually publishes (design §1).
    """
    # Two sessions on purpose: the order is seeded through the async engine the
    # API uses, and the worker's handler is synchronous, as its process is.
    confirmed = await make_confirmed_order_event(session, lines=lines)
    handle_order_confirmed(sync_session, confirmed)
    sync_session.commit()

    # The row the worker wrote to its own outbox is what its poller would
    # publish, so the envelope built from it is what order-api would receive.
    return envelope(sync_session.scalars(select(Outbox)).one())
