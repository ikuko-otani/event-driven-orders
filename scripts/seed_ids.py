"""Fixed ids for the development seed, shared by the two seed scripts.

Each seed writes one schema only (design §3.1), so the inventory seed cannot
read the ids the orders seed wrote. Deriving every id from its code with uuid5
lets both arrive at the same value without reading each other's tables.
"""

import uuid

ENTITY_CODE = "ENT-01"
CUSTOMER_CODES = ("CUST-01", "CUST-02")
ITEM_CODES = ("ITEM-01", "ITEM-02", "ITEM-03")


def seed_id(code: str) -> uuid.UUID:
    """Return the id a seeded row with this code always gets."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"urn:event-driven-orders:seed:{code}")
