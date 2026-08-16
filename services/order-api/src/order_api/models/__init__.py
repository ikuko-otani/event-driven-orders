"""Models owned by order-api (the `orders` schema).

Every model module must be imported here: Alembic autogenerate only sees the
tables registered on `Base.metadata` at import time, so a module nobody
imports silently produces an empty migration.
"""

from order_api.models.base import Base
from order_api.models.masters import Customer, Item, SalesEntity
from order_api.models.messaging import Outbox, ProcessedEvent
from order_api.models.orders import Order, OrderLine

__all__ = [
    "Base",
    "Customer",
    "Item",
    "Order",
    "OrderLine",
    "Outbox",
    "ProcessedEvent",
    "SalesEntity",
]
