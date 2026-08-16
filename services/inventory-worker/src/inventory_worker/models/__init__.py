"""Models owned by inventory-worker (the `inventory` schema).

Every model module must be imported here: Alembic autogenerate only sees the
tables registered on `Base.metadata` at import time, so a module nobody
imports silently produces an empty migration.
"""

from inventory_worker.models.base import Base
from inventory_worker.models.inventory import Inventory

__all__ = ["Base", "Inventory"]
