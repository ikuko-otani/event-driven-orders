"""Master tables owned by order-api (design §3.2)."""

import uuid

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from order_api.models.base import Base


class SalesEntity(Base):
    """A corporate entity (sales company) — the multi-entity axis of design §3.2."""

    __tablename__ = "sales_entities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(200))
