"""What inventory-worker does with one OrderConfirmed (design §5.2).

The handler announces its outcome by writing an outbox row and never touches
Kafka: the reservation rows, the consumer's processed_events claim and the
reply all land in one transaction, so the fact and the announcement of the
fact cannot come apart. Publishing directly from here would reintroduce the
dual-write problem the outbox exists to remove (design §5.4).
"""

import uuid
from typing import Any

from sqlalchemy.orm import Session

from inventory_worker.events import (
    inventory_reservation_failed_outbox,
    inventory_reserved_outbox,
)
from inventory_worker.services.reservations import (
    AlreadyReserved,
    Insufficient,
    Reserved,
    reserve_order,
)

# Which event this handler is for. The topic carries every event order-api
# produces, so a consumer applies the ones it has work for and leaves the rest
# alone (design §4.2). The name is the wire contract, quoted rather than
# imported: inventory-worker reads order-api's events, never its code (§3.1).
HANDLED_EVENT_TYPE = "OrderConfirmed"


def handle_order_confirmed(session: Session, event: dict[str, Any]) -> None:
    """Reserve the order's stock, and announce whichever outcome it reached.

    Nothing commits here: the caller owns the transaction this shares with the
    processed_events claim (design §5.2).
    """
    # Anything else on this topic is not a confirmation, and reserving stock
    # for it would hand the order's units to an event that never asked for them.
    if event["event_type"] != HANDLED_EVENT_TYPE:
        return

    # Reserve first; everything below only reports what this call decided.
    outcome = reserve_order(session, event)

    # Both replies are addressed to the order, and a failed reservation leaves
    # no row to read the ids from, so they come from the event itself.
    entity_id = uuid.UUID(event["entity_id"])
    order_id = uuid.UUID(event["payload"]["order_id"])

    # Stock changed hands, so say which rows now hold it.
    if isinstance(outcome, Reserved):
        session.add(
            inventory_reserved_outbox(
                entity_id=entity_id,
                order_id=order_id,
                reservations=outcome.reservations,
            )
        )
    # A shortage is an outcome, not an error: it commits, is never retried, and
    # is what order-api compensates on (design §5.3).
    elif isinstance(outcome, Insufficient):
        session.add(
            inventory_reservation_failed_outbox(
                entity_id=entity_id, order_id=order_id, shortages=outcome.shortages
            )
        )
    elif isinstance(outcome, AlreadyReserved):
        # A redelivery of an order that already holds its stock. Nothing
        # happened this time, so there is nothing to announce (design §3.2).
        pass
