"""What order-api does with the events inventory-worker sends back (design §5.2).

Nothing here is async: this handler runs inside the consumer process, which
blocks on the broker exactly as the poller does, so it takes a synchronous
Session and never touches the async engine the HTTP service uses.
"""

import logging
import uuid
from typing import Any

from sqlalchemy import update
from sqlalchemy.orm import Session

from order_api.models import Order

logger = logging.getLogger(__name__)

# Which inventory outcome moves the order where (design §5.7). The event names
# are the wire contract, quoted rather than imported: order-api reads
# inventory-worker's events, never its code (design §3.1).
TRANSITIONS: dict[str, str] = {"InventoryReserved": "RESERVED"}


def handle_inventory_event(session: Session, event: dict[str, Any]) -> None:
    """Apply one event from inventory.events to the order it reports on.

    Nothing commits here: the caller owns the transaction this shares with the
    processed_events claim, so the order's new status and the record that the
    event was consumed become visible together (design §5.2).
    """
    # The topic carries every event the worker produces, so a consumer applies
    # the ones it has a transition for and leaves the rest to whoever does.
    status = TRANSITIONS.get(event["event_type"])
    if status is None:
        return

    # One conditional UPDATE, never a read-then-write: the guard on CONFIRMED
    # is what makes a second delivery change nothing instead of transitioning
    # an order twice (design §5.7).
    result = session.execute(
        update(Order)
        .where(
            Order.id == uuid.UUID(event["payload"]["order_id"]),
            Order.status == "CONFIRMED",
        )
        .values(status=status)
        .returning(Order.id)
        .execution_options(synchronize_session=False)
    )

    # Zero rows folds "no such order" and "unexpected state" into one branch:
    # deterministic either way, so it is never retried and never dead-lettered.
    # Reaching it means a reordering the design believes impossible, so it is
    # reported rather than swallowed (design §5.7).
    if result.first() is None:
        logger.warning(
            "order_state_mismatch",
            extra={
                "event_type": event["event_type"],
                "order_id": event["payload"]["order_id"],
            },
        )
