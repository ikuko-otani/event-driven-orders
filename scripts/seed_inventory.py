"""Seed the inventory schema with stock for every item the orders seed wrote.

Writes the inventory schema only and imports nothing from order-api: the entity
and item ids come from seed_ids, the same derivation the orders seed used
(design §3.1). Run it after the orders seed, against the docker-compose
Postgres; the deployment runs no inventory-worker, so it is never needed there.
"""

from seed_ids import ENTITY_CODE, ITEM_CODES, seed_id
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from common.settings import DatabaseSettings
from inventory_worker.models import Inventory


def main() -> None:
    engine = create_engine(DatabaseSettings().sync_url)
    with Session(engine) as session:
        # Any stock at all means an earlier run got here, so a second run is a
        # no-op instead of a duplicate-key failure.
        if session.scalars(select(Inventory).limit(1)).first() is not None:
            print("already seeded: inventory has rows, skipping")
            return

        # One stock row per item, keyed by the ids the orders seed gave them.
        entity_id = seed_id(ENTITY_CODE)
        inventories = [
            Inventory(entity_id=entity_id, item_id=seed_id(code), quantity_on_hand=100)
            for code in ITEM_CODES
        ]
        session.add_all(inventories)
        session.commit()

        print(f"seeded: {len(inventories)} inventory rows")


if __name__ == "__main__":
    main()
