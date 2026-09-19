"""Drive a running Compose stack end to end: one order from POST to RESERVED.

A green test suite says nothing about this path, because the test fixtures
build an environment of their own. This script checks the wiring instead: it
talks to the published ports of a stack that is already up, and fails if the
order does not reach RESERVED in time.

Run it from the host, after `docker compose up -d` and `uv run poe seed`.
"""

import sys
import time
import uuid
from datetime import date, timedelta

import httpx
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from common.settings import DatabaseSettings
from order_api.models import Customer, Item, SalesEntity

BASE_URL = "http://localhost:8000"
ENTITY_HEADER = "X-Entity-Id"
RESERVED_TIMEOUT_SECONDS = 90
POLL_INTERVAL_SECONDS = 1.0


def seeded_ids() -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Return the entity, customer and item ids the seed script wrote."""
    # There are no master-data endpoints, so the ids are read where the seed
    # put them; this script is a development tool, like the seed itself.
    engine = create_engine(DatabaseSettings().sync_url)
    with Session(engine) as session:
        entity = session.scalars(select(SalesEntity).order_by(SalesEntity.code)).first()
        customer = session.scalars(select(Customer).order_by(Customer.code)).first()
        item = session.scalars(select(Item).order_by(Item.code)).first()

    # Missing rows mean the stack is up but unseeded: a setup mistake, not a
    # failure of the order path, so the message says which one it is.
    if entity is None or customer is None or item is None:
        sys.exit("no seed data found: run `uv run poe seed` first")
    return entity.id, customer.id, item.id


def main() -> None:
    entity_id, customer_id, item_id = seeded_ids()
    entity_headers = {ENTITY_HEADER: str(entity_id)}

    with httpx.Client(base_url=BASE_URL, timeout=10.0) as client:
        # Create the order: one transaction writes the order row and the outbox
        # row the poller publishes from (design §5.4).
        created = client.post(
            "/orders",
            headers={**entity_headers, "Idempotency-Key": str(uuid.uuid4())},
            json={
                "customer_id": str(customer_id),
                "currency": "JPY",
                "delivery_date": (date.today() + timedelta(days=7)).isoformat(),
                "lines": [{"item_id": str(item_id), "quantity": 1}],
            },
        )
        created.raise_for_status()
        order_id = created.json()["id"]

        # Confirm is the Saga's entry point (design §4.6); from here the work
        # crosses five processes and the broker before the status changes again.
        client.post(f"/orders/{order_id}/confirm", headers=entity_headers).raise_for_status()

        # Wait with a bound rather than a fixed sleep: that round trip has no
        # duration this script is allowed to assume.
        deadline = time.monotonic() + RESERVED_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            order = client.get(f"/orders/{order_id}", headers=entity_headers).json()
            if order["status"] == "RESERVED":
                print(f"smoke ok: order {order['order_number']} reached RESERVED")
                return

            # A refused reservation is a finished answer, not a slow one.
            if order["status"] == "RESERVATION_FAILED":
                sys.exit(f"smoke failed: {order['order_number']} was not reserved")
            time.sleep(POLL_INTERVAL_SECONDS)

    sys.exit(f"smoke failed: no RESERVED within {RESERVED_TIMEOUT_SECONDS}s")


if __name__ == "__main__":
    main()
