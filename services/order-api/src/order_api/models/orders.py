"""Transactional order tables owned by order-api (design §3.2)."""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base


class Order(Base):
    """Aggregate root of order intake (design §3.2)."""

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("entity_id", "order_number"),
        UniqueConstraint("entity_id", "idempotency_key"),
        CheckConstraint(
            "status IN ('PENDING','CONFIRMED','RESERVED','RESERVATION_FAILED','CANCELLED')",
            name="status_valid",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sales_entities.id"), index=True)
    customer_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("customers.id"), index=True)
    order_number: Mapped[str] = mapped_column(String(30))
    status: Mapped[str] = mapped_column(String(20), default="PENDING")
    idempotency_key: Mapped[str] = mapped_column(String(100))
    currency: Mapped[str] = mapped_column(String(3))
    delivery_date: Mapped[date] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OrderLine(Base):
    """One line of an order; unit_price snapshots the item's price at order time (design §3.2)."""

    __tablename__ = "order_lines"
    __table_args__ = (UniqueConstraint("order_id", "item_id"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"), index=True)
    item_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("items.id"), index=True)
    quantity: Mapped[int] = mapped_column(Integer)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
