"""What the consumer announces after reserving, with no broker running (§7.7)."""

import uuid

import pytest
from factories import make_confirmed_order_event
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from inventory_worker.handlers import handle_order_confirmed
from inventory_worker.models import InventoryReservation, Outbox


@pytest.mark.asyncio
async def test_a_reservation_announces_every_row_that_holds_stock(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await make_confirmed_order_event(db_session, lines=[(100, 3), (50, 2)])

    handle_order_confirmed(sync_session, event)
    sync_session.commit()

    row = sync_session.scalars(select(Outbox)).one()
    assert row.event_type == "InventoryReserved"
    assert row.aggregate_type == "Order"
    assert row.aggregate_id == uuid.UUID(event["payload"]["order_id"])
    assert row.entity_id == uuid.UUID(event["entity_id"])
    reservations = sync_session.scalars(select(InventoryReservation))
    held = {str(reservation.id) for reservation in reservations}
    assert {line["reservation_id"] for line in row.payload["reservations"]} == held
    assert sorted(line["quantity"] for line in row.payload["reservations"]) == [2, 3]


@pytest.mark.asyncio
async def test_a_short_line_announces_the_failure_and_reserves_nothing(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await make_confirmed_order_event(db_session, lines=[(100, 3), (1, 2)])
    lines = event["payload"]["lines"]
    short_item = next(line["item_id"] for line in lines if line["quantity"] == 2)

    handle_order_confirmed(sync_session, event)
    sync_session.commit()

    row = sync_session.scalars(select(Outbox)).one()
    assert row.event_type == "InventoryReservationFailed"
    assert row.payload["failures"] == [{"item_id": short_item, "requested": 2, "available": 1}]
    assert list(sync_session.scalars(select(InventoryReservation))) == []
