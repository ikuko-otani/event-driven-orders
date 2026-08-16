"""Seed the development Postgres with a minimal, usable master/inventory dataset.

Run manually against the docker-compose Postgres — never against the
testcontainers database, which pytest builds from scratch on every run.
Currency ("通貨1種") needs no row of its own: design §3.2 has no currency
table, only a single seeded string value carried on each order later.
"""

from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from common.settings import DatabaseSettings
from inventory_worker.models import Inventory
from order_api.models import Customer, Item, SalesEntity

ENTITY_CODE = "ENT-01"


def main() -> None:
    engine = create_engine(DatabaseSettings().sync_url)
    with Session(engine) as session:
        existing = session.execute(
            select(SalesEntity).where(SalesEntity.code == ENTITY_CODE)
        ).scalar_one_or_none()
        if existing is not None:
            print(f"already seeded: entity {ENTITY_CODE!r} exists, skipping")
            return

        entity = SalesEntity(code=ENTITY_CODE, name="Example Manufacturing Co.")
        session.add(entity)
        session.flush()

        customers = [
            Customer(entity_id=entity.id, code=f"CUST-{i:02d}", name=f"Example Customer {i}")
            for i in range(1, 3)
        ]
        items = [
            Item(
                code=f"ITEM-{i:02d}",
                name=f"Example Item {i}",
                list_price=Decimal("1000.00"),
            )
            for i in range(1, 4)
        ]
        session.add_all(customers)
        session.add_all(items)
        session.flush()

        inventories = [
            Inventory(entity_id=entity.id, item_id=item.id, quantity_on_hand=100) for item in items
        ]
        session.add_all(inventories)
        session.commit()

        print(
            f"seeded: 1 entity, {len(customers)} customers, "
            f"{len(items)} items, {len(inventories)} inventory rows"
        )


if __name__ == "__main__":
    main()
