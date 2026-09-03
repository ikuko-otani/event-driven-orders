"""Reserving an order's stock, with no broker running (design §7.7)."""

import uuid
from collections.abc import Sequence
from typing import Any

import pytest
from factories import (
    make_customer,
    make_inventory,
    make_item,
    make_order,
    make_sales_entity,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.envelope import envelope
from inventory_worker.models import Inventory, InventoryReservation
from inventory_worker.services.reservations import (
    AlreadyReserved,
    Insufficient,
    Reserved,
    reserve_order,
)
from order_api.events import order_confirmed_outbox
from order_api.models import Item


async def _confirmed_order_event(
    session: AsyncSession, *, lines: Sequence[tuple[int, int]]
) -> dict[str, Any]:
    """Seed stock and a CONFIRMED order, and return the event its confirm published.

    Each pair is one line: the stock that exists, and the quantity ordered.
    """
    entity = await make_sales_entity(session)
    customer = await make_customer(session, entity=entity)
    ordered: list[tuple[Item, int]] = []
    for index, (on_hand, quantity) in enumerate(lines):
        item = await make_item(session, code=f"ITEM-{index:02d}")
        await make_inventory(session, entity=entity, item=item, quantity_on_hand=on_hand)
        ordered.append((item, quantity))
    order = await make_order(
        session, entity=entity, customer=customer, lines=ordered, status="CONFIRMED"
    )
    row = order_confirmed_outbox(order)
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return envelope(row)


def _redelivered_as_a_new_event(event: dict[str, Any]) -> dict[str, Any]:
    """The same OrderConfirmed carrying a fresh event_id (design §5.7).

    Two confirms racing each other write two outbox rows with distinct ids and
    an identical payload — the one duplicate processed_events cannot absorb,
    because it deduplicates on exactly the field that differs.
    """
    return {**event, "event_id": str(uuid.uuid4())}


@pytest.mark.asyncio
async def test_every_line_is_reserved_and_its_counter_raised(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await _confirmed_order_event(db_session, lines=[(100, 3), (50, 2)])

    outcome = reserve_order(sync_session, event)
    sync_session.commit()

    assert isinstance(outcome, Reserved)
    stored = list(sync_session.scalars(select(InventoryReservation)))
    assert sorted(row.quantity for row in stored) == [2, 3]
    assert {row.created_by_event for row in stored} == {uuid.UUID(event["event_id"])}
    assert sorted(sync_session.scalars(select(Inventory.quantity_reserved))) == [2, 3]


@pytest.mark.asyncio
async def test_one_short_line_reserves_nothing_at_all(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await _confirmed_order_event(db_session, lines=[(100, 3), (1, 2)])

    outcome = reserve_order(sync_session, event)
    sync_session.commit()

    assert isinstance(outcome, Insufficient)
    assert [(s.requested, s.available) for s in outcome.shortages] == [(2, 1)]
    assert list(sync_session.scalars(select(InventoryReservation))) == []
    assert sorted(sync_session.scalars(select(Inventory.quantity_reserved))) == [0, 0]


@pytest.mark.asyncio
async def test_a_second_confirm_of_the_same_order_reserves_no_more_stock(
    db_session: AsyncSession, sync_session: Session
) -> None:
    # (stock on hand, quantity ordered) per line — stocked well above the order,
    # so the redelivery still passes the availability check and reaches the index.
    event = await _confirmed_order_event(db_session, lines=[(100, 3), (50, 2)])
    reserve_order(sync_session, event)
    sync_session.commit()

    outcome = reserve_order(sync_session, _redelivered_as_a_new_event(event))
    sync_session.commit()

    assert isinstance(outcome, AlreadyReserved)
    assert len(list(sync_session.scalars(select(InventoryReservation)))) == 2
    assert sorted(sync_session.scalars(select(Inventory.quantity_reserved))) == [2, 3]


@pytest.mark.asyncio
async def test_a_duplicate_is_not_reported_as_a_shortage(
    db_session: AsyncSession, sync_session: Session
) -> None:
    # Stocked to exactly the order, so the first reservation leaves no room and
    # the redelivery reads as short — a failure this very order already fixed.
    event = await _confirmed_order_event(db_session, lines=[(3, 3)])
    reserve_order(sync_session, event)
    sync_session.commit()

    outcome = reserve_order(sync_session, _redelivered_as_a_new_event(event))
    sync_session.commit()

    assert isinstance(outcome, AlreadyReserved)
    assert len(list(sync_session.scalars(select(InventoryReservation)))) == 1
    assert list(sync_session.scalars(select(Inventory.quantity_reserved))) == [3]
