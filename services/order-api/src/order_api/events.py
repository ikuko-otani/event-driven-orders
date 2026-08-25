"""Outbox rows for the events order-api produces (design §4.1, §4.3)."""

from typing import Any

from order_api.models import Order, Outbox

ORDER_CONFIRMED = "OrderConfirmed"


def order_confirmed_payload(order: Order) -> dict[str, Any]:
    """Everything inventory-worker needs to reserve, carried in the event itself."""
    return {
        "order_id": str(order.id),
        "order_number": order.order_number,
        "customer_id": str(order.customer_id),
        "delivery_date": order.delivery_date.isoformat(),
        "currency": order.currency,
        "lines": [
            {"item_id": str(line.item_id), "quantity": line.quantity} for line in order.lines
        ],
    }


def order_confirmed_outbox(order: Order) -> Outbox:
    """The outbox row written in the same transaction as the PENDING → CONFIRMED update."""
    return Outbox(
        entity_id=order.entity_id,
        aggregate_type="Order",
        aggregate_id=order.id,
        event_type=ORDER_CONFIRMED,
        payload=order_confirmed_payload(order),
    )
