"""HTTP-level tests for POST /orders: creation, price snapshot, validation."""

from decimal import Decimal

import pytest
from factories import create_customer, create_item, create_sales_entity
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_valid_order_creation_returns_201_with_snapshotted_prices(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    item = await create_item(db_session, list_price=Decimal("1234.50"))
    await db_session.commit()

    response = await api_client.post(
        "/orders",
        headers={"X-Entity-Id": str(entity.id), "Idempotency-Key": "key-1"},
        json={
            "customer_id": str(customer.id),
            "currency": "JPY",
            "delivery_date": "2026-09-01",
            "lines": [{"item_id": str(item.id), "quantity": 2}],
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["order_number"].startswith("ORD-")
    assert body["status"] == "PENDING"
    assert body["lines"] == [{"item_id": str(item.id), "quantity": 2, "unit_price": "1234.50"}]
