"""Declarative base for the `orders` schema — owned by order-api (design §3.1)."""

from common.db import make_metadata
from sqlalchemy.orm import DeclarativeBase

SCHEMA = "orders"


class Base(DeclarativeBase):
    """Every orders-schema model inherits this; no table of ours lives elsewhere."""

    metadata = make_metadata(SCHEMA)
