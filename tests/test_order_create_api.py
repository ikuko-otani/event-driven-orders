"""HTTP-level tests for POST /orders: creation, price snapshot, validation."""

import uuid
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


@pytest.mark.asyncio
async def test_order_creation_with_no_lines_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    await db_session.commit()

    response = await api_client.post(
        "/orders",
        headers={"X-Entity-Id": str(entity.id), "Idempotency-Key": "key-2"},
        json={
            "customer_id": str(customer.id),
            "currency": "JPY",
            "delivery_date": "2026-09-01",
            "lines": [],
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_order_creation_over_line_cap_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    await db_session.commit()

    lines = [{"item_id": str(uuid.uuid4()), "quantity": 1} for _ in range(51)]
    response = await api_client.post(
        "/orders",
        headers={"X-Entity-Id": str(entity.id), "Idempotency-Key": "key-3"},
        json={
            "customer_id": str(customer.id),
            "currency": "JPY",
            "delivery_date": "2026-09-01",
            "lines": lines,
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_order_creation_with_duplicate_item_id_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    item = await create_item(db_session)
    await db_session.commit()

    response = await api_client.post(
        "/orders",
        headers={"X-Entity-Id": str(entity.id), "Idempotency-Key": "key-4"},
        json={
            "customer_id": str(customer.id),
            "currency": "JPY",
            "delivery_date": "2026-09-01",
            "lines": [
                {"item_id": str(item.id), "quantity": 1},
                {"item_id": str(item.id), "quantity": 2},
            ],
        },
    )

    assert response.status_code == 422
