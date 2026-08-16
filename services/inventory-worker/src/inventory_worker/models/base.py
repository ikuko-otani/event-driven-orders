"""Declarative base for the `inventory` schema — owned by inventory-worker (design §3.1)."""

from sqlalchemy.orm import DeclarativeBase

from common.db import make_metadata

SCHEMA = "inventory"


class Base(DeclarativeBase):
    """Every inventory-schema model inherits this; no table of ours lives elsewhere."""

    metadata = make_metadata(SCHEMA)
