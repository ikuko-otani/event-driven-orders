"""Reserving an order's stock: every line or none of them (design §5.2).

Nothing here commits. The caller owns the transaction, so the reservation
rows, the stock counters and the consumer's processed_events row all become
visible together. Insufficient stock is reported in the return value and
never raised: an exception could not be told apart from a technical failure,
and the retry machinery would repeat a permanent business condition forever
(design §5.3).
"""

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from inventory_worker.models import Inventory, InventoryReservation


@dataclass(frozen=True)
class Reserved:
    """Every line was reserved; these rows now hold the stock."""

    reservations: list[InventoryReservation]


@dataclass(frozen=True)
class AlreadyReserved:
    """A duplicate delivery: this order already holds its stock (design §3.2)."""


@dataclass(frozen=True)
class Shortage:
    """One line that could not be reserved, shaped as §4.3's failure payload needs."""

    item_id: uuid.UUID
    requested: int
    available: int


@dataclass(frozen=True)
class Insufficient:
    """A business failure: at least one line was short, so nothing was reserved."""

    shortages: list[Shortage]


ReservationOutcome = Reserved | AlreadyReserved | Insufficient


def _lock_inventory(
    session: Session, *, entity_id: uuid.UUID, item_ids: list[uuid.UUID]
) -> dict[uuid.UUID, Inventory]:
    """Lock this order's stock rows, always in item_id order (design §3.2).

    Two orders wanting the same two items would deadlock if they took the two
    locks in opposite orders, so the order is fixed for every caller. One
    query is enough: PostgreSQL sorts the rows before it locks them, so the
    locks are taken in the sorted order the plan produced.
    """
    rows = session.scalars(
        select(Inventory)
        .where(Inventory.entity_id == entity_id, Inventory.item_id.in_(item_ids))
        .order_by(Inventory.item_id)
        .with_for_update()
    )
    return {row.item_id: row for row in rows}


def reserve_order(session: Session, event: dict[str, Any]) -> ReservationOutcome:
    """Reserve every line of one OrderConfirmed, or leave the stock untouched.

    Availability is judged for all lines while their rows are locked, and the
    writes start only once every line has passed: reserving line by line would
    leave stock held for an order that is about to fail (design §5.2).
    """
    entity_id = uuid.UUID(event["entity_id"])
    event_id = uuid.UUID(event["event_id"])
    payload = event["payload"]
    order_id = uuid.UUID(payload["order_id"])
    lines: dict[uuid.UUID, int] = {
        uuid.UUID(line["item_id"]): line["quantity"] for line in payload["lines"]
    }
    stock = _lock_inventory(session, entity_id=entity_id, item_ids=list(lines))

    shortages: list[Shortage] = []
    for item_id, quantity in lines.items():
        row = stock.get(item_id)
        available = 0 if row is None else row.quantity_on_hand - row.quantity_reserved
        if available < quantity:
            shortages.append(Shortage(item_id=item_id, requested=quantity, available=available))
    if shortages:
        return Insufficient(shortages=shortages)

    reservations: list[InventoryReservation] = []
    for item_id, quantity in lines.items():
        stock[item_id].quantity_reserved += quantity
        reservation = InventoryReservation(
            entity_id=entity_id,
            order_id=order_id,
            item_id=item_id,
            quantity=quantity,
            created_by_event=event_id,
        )
        session.add(reservation)
        reservations.append(reservation)
    session.flush()
    return Reserved(reservations=reservations)
