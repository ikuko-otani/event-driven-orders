"""Seed the orders schema with a minimal, usable master dataset.

Writes the orders schema only, so it runs wherever the order-api package is
installed: on the host against the docker-compose Postgres, and inside the api
image as a one-off task against a database the host cannot reach. Never run it
against the testcontainers database, which pytest builds from scratch.

Every id comes from seed_ids, so the inventory seed finds these rows without
reading this schema (design §3.1). The ids are printed, because a deployed
database may be reachable from nowhere else.

Currency needs no row of its own: design §3.2 has no currency table, only a
single ISO 4217 value seeded once and carried on each order.
"""

from decimal import Decimal

from seed_ids import CUSTOMER_CODES, ENTITY_CODE, ITEM_CODES, seed_id
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from common.settings import DatabaseSettings
from order_api.models import Customer, Item, SalesEntity


def main() -> None:
    engine = create_engine(DatabaseSettings().sync_url)
    with Session(engine) as session:
        # Look the entity up by code, so a second run is a no-op instead of a
        # duplicate-key failure, whatever ids an older seed gave its rows.
        existing = session.execute(
            select(SalesEntity).where(SalesEntity.code == ENTITY_CODE)
        ).scalar_one_or_none()
        if existing is not None:
            print(f"already seeded: entity {ENTITY_CODE!r} exists, skipping")
            return

        # The customers reference the entity, so it is written first.
        entity = SalesEntity(
            id=seed_id(ENTITY_CODE), code=ENTITY_CODE, name="Example Manufacturing Co."
        )
        session.add(entity)
        session.flush()

        # Write the masters with the same ids the inventory seed derives.
        customers = [
            Customer(id=seed_id(code), entity_id=entity.id, code=code, name=f"Example Customer {n}")
            for n, code in enumerate(CUSTOMER_CODES, start=1)
        ]
        items = [
            Item(
                id=seed_id(code), code=code, name=f"Example Item {n}", list_price=Decimal("1000.00")
            )
            for n, code in enumerate(ITEM_CODES, start=1)
        ]
        session.add_all(customers)
        session.add_all(items)
        session.commit()

        # Print the ids: against a deployed database this is the only way to learn them.
        print(f"seeded: entity {entity.code} {entity.id}")
        for customer in customers:
            print(f"seeded: {customer.code} {customer.id}")
        for item in items:
            print(f"seeded: {item.code} {item.id}")


if __name__ == "__main__":
    main()
