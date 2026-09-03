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
