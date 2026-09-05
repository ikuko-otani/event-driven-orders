"""Outbox rows for the events inventory-worker produces (design §4.1, §4.3)."""

import uuid
from collections.abc import Sequence

from inventory_worker.models import InventoryReservation, Outbox
from inventory_worker.services.reservations import Shortage

INVENTORY_RESERVED = "InventoryReserved"
INVENTORY_RESERVATION_FAILED = "InventoryReservationFailed"


def inventory_reserved_outbox(
    *, entity_id: uuid.UUID, order_id: uuid.UUID, reservations: Sequence[InventoryReservation]
) -> Outbox:
    """The row written in the same transaction as the reservations it announces.

    aggregate_id is the order, never an inventory-side id: this reply is a step
    in the order's Saga, and keying it on a reservation would spread one order's
    events across partitions and lose the order they must be read in (§4.2).
    Naming each reservation row lets order-api answer from the event alone,
    without ever reading the inventory schema (design §3.1).
    """
    return Outbox(
        entity_id=entity_id,
        aggregate_type="Order",
        aggregate_id=order_id,
        event_type=INVENTORY_RESERVED,
        payload={
            "order_id": str(order_id),
            "reservations": [
                {
                    "item_id": str(reservation.item_id),
                    "quantity": reservation.quantity,
                    "reservation_id": str(reservation.id),
                }
                for reservation in reservations
            ],
        },
    )


def inventory_reservation_failed_outbox(
    *, entity_id: uuid.UUID, order_id: uuid.UUID, shortages: Sequence[Shortage]
) -> Outbox:
    """The row written when a business failure is the outcome (design §5.3).

    A shortage commits like any other outcome: the reservation wrote no rows,
    so there is nothing to undo, and this event is the fact order-api
    compensates on. Both numbers travel, not just the fact of the failure —
    splitting an order into a fulfillable part and a backorder needs the gap,
    and order-api cannot look the stock up for itself (design §4.3).
    """
    return Outbox(
        entity_id=entity_id,
        aggregate_type="Order",
        aggregate_id=order_id,
        event_type=INVENTORY_RESERVATION_FAILED,
        payload={
            "order_id": str(order_id),
            "failures": [
                {
                    "item_id": str(shortage.item_id),
                    "requested": shortage.requested,
                    "available": shortage.available,
                }
                for shortage in shortages
            ],
        },
    )
