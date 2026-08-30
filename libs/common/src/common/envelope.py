"""The event envelope every topic shares (design §4.1)."""

from typing import Any

from common.messaging import OutboxMixin


def envelope(row: OutboxMixin) -> dict[str, Any]:
    """Remap one outbox row into the envelope; the poller reads no content."""
    return {
        "event_id": str(row.id),
        "event_type": row.event_type,
        "event_version": row.event_version,
        "occurred_at": row.created_at.isoformat(),
        "entity_id": str(row.entity_id),
        "aggregate_type": row.aggregate_type,
        "aggregate_id": str(row.aggregate_id),
        "payload": row.payload,
    }
