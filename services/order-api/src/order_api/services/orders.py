"""Order-creation business logic: numbering, price snapshot, validation."""

import uuid
from collections import Counter

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.models import Customer, Item, Order, OrderLine
from order_api.schemas.orders import OrderCreate


async def _next_order_number(session: AsyncSession) -> str:
    result = await session.execute(select(func.nextval("orders.order_number_seq")))
    value = result.scalar_one()
    return f"ORD-{value:06d}"


async def create_order(
    session: AsyncSession,
    *,
    entity_id: uuid.UUID,
    idempotency_key: str,
    body: OrderCreate,
) -> Order:
    duplicate_items = [
        item for item, count in Counter(line.item_id for line in body.lines).items() if count > 1
    ]
    if duplicate_items:
        raise HTTPException(422, detail=f"duplicate item_id in lines: {duplicate_items}")

    customer = await session.get(Customer, body.customer_id)
    if customer is None or customer.entity_id != entity_id:
        raise HTTPException(422, detail="customer_id does not exist for this entity")

    item_ids = [line.item_id for line in body.lines]
    items_result = await session.execute(select(Item).where(Item.id.in_(item_ids)))
    items_by_id = {item.id: item for item in items_result.scalars()}
    missing = set(item_ids) - items_by_id.keys()
    if missing:
        raise HTTPException(422, detail=f"item_id not found: {missing}")

    order_number = await _next_order_number(session)

    order = Order(
        entity_id=entity_id,
        customer_id=body.customer_id,
        order_number=order_number,
        idempotency_key=idempotency_key,
        currency=body.currency,
        delivery_date=body.delivery_date,
    )
    order.lines = [
        OrderLine(
            item_id=line.item_id,
            quantity=line.quantity,
            unit_price=items_by_id[line.item_id].list_price,
        )
        for line in body.lines
    ]
    session.add(order)
    await session.flush()
    return order
