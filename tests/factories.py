"""Factories for domain state that satisfies design §1's reproducibility rule.

Each helper takes the session and any overrides, adds the row, and returns the
ORM instance with its primary key populated after a flush — never committed
here, so callers control the transaction boundary.
"""

from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from inventory_worker.models import Inventory
from order_api.models import Customer, Item, SalesEntity


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
