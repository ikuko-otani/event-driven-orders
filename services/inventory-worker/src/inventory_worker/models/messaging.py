"""Messaging tables owned by inventory-worker (the `inventory` schema, design §3.1)."""

from common.messaging import OutboxMixin, ProcessedEventMixin
from inventory_worker.models.base import Base


class Outbox(OutboxMixin, Base):
    """inventory-worker's own outbox; its poller publishes these rows to `inventory.events`."""


class ProcessedEvent(ProcessedEventMixin, Base):
    """inventory-worker's dedup ledger for events it consumes from `orders.events`."""
