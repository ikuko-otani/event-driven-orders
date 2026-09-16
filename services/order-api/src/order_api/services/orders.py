"""Order-creation business logic: numbering, price snapshot, validation."""

import hashlib
import uuid
from collections import Counter
from collections.abc import Sequence

from fastapi import HTTPException
from opentelemetry import trace
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from order_api.events import order_confirmed_outbox
from order_api.models import Customer, Item, Order, OrderLine
from order_api.schemas.orders import OrderCreate, OrderLineCreate, OrderUpdate

IDEMPOTENCY_KEY_CONSTRAINT = "uq_orders_entity_id_idempotency_key"
tracer = trace.get_tracer(__name__)


async def next_order_number(session: AsyncSession) -> str:
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


def fingerprint(body: OrderCreate) -> str:
    canonical = body.model_dump_json()
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_order(session: AsyncSession, *, entity_id: uuid.UUID, order_id: uuid.UUID) -> Order:
    """Load one order with its lines, scoped to the caller's entity."""
    result = await session.execute(
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.id == order_id, Order.entity_id == entity_id)
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(404, detail="order not found")
    return order


async def list_orders(
    session: AsyncSession,
    *,
    entity_id: uuid.UUID,
    status: str | None = None,
    customer_id: uuid.UUID | None = None,
    limit: int = 20,
    offset: int = 0,
) -> Sequence[Order]:
    """List one entity's orders, newest first, without their lines."""
    stmt = select(Order).where(Order.entity_id == entity_id)
    if status is not None:
        stmt = stmt.where(Order.status == status)
    if customer_id is not None:
        stmt = stmt.where(Order.customer_id == customer_id)
    stmt = stmt.order_by(Order.created_at.desc(), Order.order_number.desc())
    result = await session.execute(stmt.limit(limit).offset(offset))
    return result.scalars().all()


async def _resolve_line_items(
    session: AsyncSession, lines: Sequence[OrderLineCreate]
) -> dict[uuid.UUID, Item]:
    """Validate the requested lines and return the items they name, keyed by id."""
    duplicates = [
        item_id for item_id, count in Counter(line.item_id for line in lines).items() if count > 1
    ]
    if duplicates:
        raise HTTPException(422, detail=f"duplicate item_id in lines: {duplicates}")

    item_ids = [line.item_id for line in lines]
    result = await session.execute(select(Item).where(Item.id.in_(item_ids)))
    items_by_id = {item.id: item for item in result.scalars()}
    missing = set(item_ids) - items_by_id.keys()
    if missing:
        raise HTTPException(422, detail=f"item_id not found: {missing}")
    return items_by_id


async def create_order(
    session: AsyncSession,
    *,
    entity_id: uuid.UUID,
    idempotency_key: str,
    body: OrderCreate,
) -> tuple[Order, bool]:
    customer = await session.get(Customer, body.customer_id)
    if customer is None or customer.entity_id != entity_id:
        raise HTTPException(422, detail="customer_id does not exist for this entity")

    items_by_id = await _resolve_line_items(session, body.lines)

    order_number = await next_order_number(session)
    request_fingerprint = fingerprint(body)

    order = Order(
        entity_id=entity_id,
        customer_id=body.customer_id,
        order_number=order_number,
        idempotency_key=idempotency_key,
        request_fingerprint=request_fingerprint,
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
        if existing.request_fingerprint != request_fingerprint:
            raise HTTPException(
                422, detail="idempotency key reused with a different request body"
            ) from err
        return existing, False
    return order, True


async def update_order(
    session: AsyncSession,
    *,
    entity_id: uuid.UUID,
    order_id: uuid.UUID,
    body: OrderUpdate,
) -> Order:
    """Update a PENDING order in place; refuse it once confirmed (design §4.6)."""
    if body.delivery_date is None and body.lines is None:
        raise HTTPException(422, detail="no updatable fields provided")

    result = await session.execute(
        select(Order)
        .options(selectinload(Order.lines))
        .where(Order.id == order_id, Order.entity_id == entity_id)
        .with_for_update()
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(404, detail="order not found")
    if order.status != "PENDING":
        raise HTTPException(409, detail=f"order is {order.status} and no longer editable")

    if body.delivery_date is not None:
        order.delivery_date = body.delivery_date

    if body.lines is not None:
        items_by_id = await _resolve_line_items(session, body.lines)
        order.lines.clear()
        await session.flush()
        order.lines = [
            OrderLine(
                item_id=line.item_id,
                quantity=line.quantity,
                unit_price=items_by_id[line.item_id].list_price,
            )
            for line in body.lines
        ]
        await session.flush()
    return order


async def confirm_order(
    session: AsyncSession, *, entity_id: uuid.UUID, order_id: uuid.UUID
) -> Order:
    """Take the PENDING → CONFIRMED transition, emitting OrderConfirmed exactly once.

    The transition is one conditional UPDATE, never a read-then-write: two
    concurrent confirms would both observe PENDING and write two outbox rows
    carrying distinct event_ids, the one duplicate processed_events cannot
    absorb (design §5.7).
    """
    with tracer.start_as_current_span("order.confirm") as span:
        span.set_attribute("order.id", str(order_id))
        result = await session.execute(
            update(Order)
            .where(
                Order.id == order_id,
                Order.entity_id == entity_id,
                Order.status == "PENDING",
            )
            .values(status="CONFIRMED")
            .returning(Order.id)
            .execution_options(synchronize_session=False)
        )
        transitioned = result.scalar_one_or_none() is not None
        span.set_attribute("order.transitioned", transitioned)

        order = await get_order(session, entity_id=entity_id, order_id=order_id)
        if transitioned:
            session.add(order_confirmed_outbox(order))
        return order
