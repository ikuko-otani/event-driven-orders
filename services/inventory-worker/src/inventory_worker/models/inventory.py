"""The inventory schema's live-stock table (design §3.2)."""

import uuid

from sqlalchemy import Integer, text
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
