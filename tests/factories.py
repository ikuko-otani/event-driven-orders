"""Factories for domain state that satisfies design §1's reproducibility rule.

Each helper takes the session and any overrides, adds the row, and returns the
ORM instance with its primary key populated after a flush — never committed
here, so callers control the transaction boundary.
"""

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from inventory_worker.models import Inventory
from order_api.models import Customer, Item, Order, OrderLine, SalesEntity
from order_api.services.orders import next_order_number


async def create_sales_entity(
    session: AsyncSession,
    *,
    code: str = "ENT-01",
    name: str = "Example Manufacturing Co.",
) -> SalesEntity:
    entity = SalesEntity(code=code, name=name)
    session.add(entity)
    await session.flush()
    return entity


async def create_customer(
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


async def create_item(
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


async def create_inventory(
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


async def create_order(
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
