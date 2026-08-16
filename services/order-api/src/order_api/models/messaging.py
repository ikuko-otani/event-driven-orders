"""Messaging tables owned by order-api (the `orders` schema, design §3.1)."""

from common.messaging import OutboxMixin, ProcessedEventMixin
from order_api.models.base import Base


class Outbox(OutboxMixin, Base):
    """order-api's own outbox; its poller publishes these rows to `orders.events`."""


class ProcessedEvent(ProcessedEventMixin, Base):
    """order-api's dedup ledger for events it consumes from `inventory.events`."""
