"""The inventory schema's live-stock and reservation tables (design §3.2)."""

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from inventory_worker.models.base import Base


class Inventory(Base):
    """One row per (entity_id, item_id): each sales entity holds its own stock.

    entity_id/item_id reference order-api's masters by convention only — no FK
    is possible across schemas (design §3.1).
    """

    __tablename__ = "inventory"

    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    quantity_on_hand: Mapped[int] = mapped_column(Integer)
    quantity_reserved: Mapped[int] = mapped_column(Integer, server_default=text("0"))


class InventoryReservation(Base):
    """One reservation per (order_id, item_id): an audit trail of held stock (design §3.2).

    order_id/item_id are carried by value only — order_id points across schemas
    (design §3.1 forbids that FK). entity_id/item_id together, though, are a
    same-schema reference to the inventory row this reservation was taken from,
    so that pair is enforced as a real composite FK.
    """

    __tablename__ = "inventory_reservations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["entity_id", "item_id"], ["inventory.entity_id", "inventory.item_id"]
        ),
        CheckConstraint("status IN ('ACTIVE','RELEASED')", name="status_valid"),
        Index(
            "uq_inventory_reservations_active_order_item",
            "order_id",
            "item_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), index=True)
    quantity: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), default="ACTIVE")
    created_by_event: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
