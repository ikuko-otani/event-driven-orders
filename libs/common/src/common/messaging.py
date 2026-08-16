"""The outbox table, shared in shape by both services (design §3.1).

Both order-api and inventory-worker produce events, so both own an `outbox`
table of an identical shape. The definition lives here once because its column
names are a contract with the poller (design §5.4), which reads and updates
them by name; a second copy would let the two drift apart unnoticed. Only the
definition is shared — each service creates its own table in its own schema.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column


class OutboxMixin:
    """Transactional-outbox rows, written in the same transaction as the business change."""

    __tablename__ = "outbox"

    @declared_attr.directive
    def __table_args__(cls) -> tuple[Any, ...]:
        # The poller's hot query, as a partial index: a row leaves the index
        # once published, so it stays near-empty in steady state (design §5.4).
        # Built per subclass because an Index object cannot be shared.
        return (
            Index(
                "ix_outbox_unpublished",
                "created_at",
                postgresql_where=text("published_at IS NULL AND quarantined_at IS NULL"),
            ),
        )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    aggregate_type: Mapped[str] = mapped_column(String(50))
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(50))
    event_version: Mapped[int] = mapped_column(Integer, server_default=text("1"))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
