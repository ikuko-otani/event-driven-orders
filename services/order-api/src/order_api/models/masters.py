"""Master tables owned by order-api (design §3.2)."""

import uuid

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base


class SalesEntity(Base):
    """A corporate entity (sales company) — the multi-entity axis of design §3.2."""

    __tablename__ = "sales_entities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(20), unique=True)
    name: Mapped[str] = mapped_column(String(200))


class Customer(Base):
    """A customer, owned by one sales entity's ledger (design §3.2)."""

    __tablename__ = "customers"
    __table_args__ = (UniqueConstraint("entity_id", "code"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sales_entities.id"), index=True)
    code: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(200))
