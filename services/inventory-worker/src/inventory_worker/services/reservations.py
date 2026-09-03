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

from inventory_worker.models import InventoryReservation


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
