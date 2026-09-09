"""Reserving an order's stock, with no broker running (design §7.7)."""

import uuid
from collections.abc import Callable

import pytest
from factories import make_confirmed_order_event, make_redelivery
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.consumer import PermanentFailure
from inventory_worker.models import Inventory, InventoryReservation
from inventory_worker.services.reservations import (
    AlreadyReserved,
    Insufficient,
    Reserved,
    reserve_order,
)


@pytest.mark.asyncio
async def test_every_line_is_reserved_and_its_counter_raised(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await make_confirmed_order_event(db_session, lines=[(100, 3), (50, 2)])

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
    event = await make_confirmed_order_event(db_session, lines=[(100, 3), (1, 2)])

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
    event = await make_confirmed_order_event(db_session, lines=[(100, 3), (50, 2)])
    reserve_order(sync_session, event)
    sync_session.commit()

    outcome = reserve_order(sync_session, make_redelivery(event))
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
    event = await make_confirmed_order_event(db_session, lines=[(3, 3)])
    reserve_order(sync_session, event)
    sync_session.commit()

    outcome = reserve_order(sync_session, make_redelivery(event))
    sync_session.commit()

    assert isinstance(outcome, AlreadyReserved)
    assert len(list(sync_session.scalars(select(InventoryReservation)))) == 1
    assert list(sync_session.scalars(select(Inventory.quantity_reserved))) == [3]


@pytest.mark.asyncio
async def test_a_reservation_holds_its_stock_rows_while_it_judges(
    db_session: AsyncSession,
    sync_session: Session,
    sync_session_factory: Callable[[], Session],
) -> None:
    # A short order writes nothing at all, so FOR UPDATE is the only thing that
    # can still be holding a lock. Going through a successful reservation would
    # prove nothing: its UPDATE locks the same row on its own (design §3.2).
    event = await make_confirmed_order_event(db_session, lines=[(1, 3)])
    assert isinstance(reserve_order(sync_session, event), Insufficient)

    # NOWAIT turns "wait for the lock" into "fail immediately", so the lock can
    # be observed without a second thread that would simply block forever.
    with sync_session_factory() as other, pytest.raises(OperationalError):
        other.scalars(select(Inventory).with_for_update(nowait=True)).all()


@pytest.mark.asyncio
async def test_a_line_whose_item_has_no_inventory_row_is_a_permanent_failure(
    db_session: AsyncSession, sync_session: Session
) -> None:
    # Deleting the seeded row is how a test says "this item was never stocked":
    # the factory always creates one, because every other case needs it.
    event = await make_confirmed_order_event(db_session, lines=[(100, 3)])
    sync_session.execute(delete(Inventory))
    sync_session.commit()

    with pytest.raises(PermanentFailure):
        reserve_order(sync_session, event)
