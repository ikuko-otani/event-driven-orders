"""Order-creation business logic: numbering, price snapshot, validation."""

import hashlib
import uuid
from collections import Counter

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from order_api.models import Customer, Item, Order, OrderLine
from order_api.schemas.orders import OrderCreate

IDEMPOTENCY_KEY_CONSTRAINT = "uq_orders_entity_id_idempotency_key"


async def _next_order_number(session: AsyncSession) -> str:
    result = await session.execute(select(func.nextval("orders.order_number_seq")))
    value = result.scalar_one()
    return f"ORD-{value:06d}"


async def _get_by_idempotency_key(
    session: AsyncSession, *, entity_id: uuid.UUID, idempotency_key: str
) -> Order:
    result = await session.execute(
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.entity_id == entity_id, Order.idempotency_key == idempotency_key)
    )
    return result.scalar_one()


def _fingerprint(body: OrderCreate) -> str:
    canonical = body.model_dump_json()
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_order(
    session: AsyncSession,
    *,
    entity_id: uuid.UUID,
    idempotency_key: str,
    body: OrderCreate,
) -> tuple[Order, bool]:
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
    fingerprint = _fingerprint(body)

    order = Order(
        entity_id=entity_id,
        customer_id=body.customer_id,
        order_number=order_number,
        idempotency_key=idempotency_key,
        request_fingerprint=fingerprint,
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
    try:
        await session.flush()
    except IntegrityError as err:
        cause = getattr(err.orig, "__cause__", None)
        constraint_name = getattr(cause, "constraint_name", None)
        if constraint_name != IDEMPOTENCY_KEY_CONSTRAINT:
            raise
        await session.rollback()
        existing = await _get_by_idempotency_key(
            session, entity_id=entity_id, idempotency_key=idempotency_key
        )
        if existing.request_fingerprint != fingerprint:
            raise HTTPException(
                422, detail="idempotency key reused with a different request body"
            ) from err
        return existing, False
    return order, True
