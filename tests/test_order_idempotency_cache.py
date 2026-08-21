"""Tests specific to the Redis response cache layer (design §4.4)."""

import pytest
from factories import create_customer, create_item, create_sales_entity
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_cache_hit_bypasses_the_db_fingerprint_check(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    item = await create_item(db_session)
    await db_session.commit()

    headers = {"X-Entity-Id": str(entity.id), "Idempotency-Key": "cache-hit-key"}
    payload = {
        "customer_id": str(customer.id),
        "currency": "JPY",
        "delivery_date": "2026-09-01",
        "lines": [{"item_id": str(item.id), "quantity": 1}],
    }

    first = await api_client.post("/orders", headers=headers, json=payload)
    assert first.status_code == 201

    await db_session.execute(
        text("UPDATE orders.orders SET request_fingerprint = 'corrupted' WHERE id = :id"),
        {"id": first.json()["id"]},
    )
    await db_session.commit()

    second = await api_client.post("/orders", headers=headers, json=payload)

    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]


@pytest.mark.asyncio
async def test_cached_response_carries_a_24_hour_ttl(
    db_session: AsyncSession, api_client: AsyncClient, redis_client: Redis
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    item = await create_item(db_session)
    await db_session.commit()

    headers = {"X-Entity-Id": str(entity.id), "Idempotency-Key": "ttl-key"}
    payload = {
        "customer_id": str(customer.id),
        "currency": "JPY",
        "delivery_date": "2026-09-01",
        "lines": [{"item_id": str(item.id), "quantity": 1}],
    }
    await api_client.post("/orders", headers=headers, json=payload)

    ttl = await redis_client.ttl(f"idempotency:{entity.id}:ttl-key")

    assert 0 < ttl <= 24 * 60 * 60


@pytest.mark.asyncio
async def test_cache_miss_falls_back_to_the_db_constraint(
    db_session: AsyncSession, api_client: AsyncClient, redis_client: Redis
) -> None:
    entity = await create_sales_entity(db_session)
    customer = await create_customer(db_session, entity=entity)
    item = await create_item(db_session)
    await db_session.commit()

    headers = {"X-Entity-Id": str(entity.id), "Idempotency-Key": "fallback-key"}
    payload = {
        "customer_id": str(customer.id),
        "currency": "JPY",
        "delivery_date": "2026-09-01",
        "lines": [{"item_id": str(item.id), "quantity": 1}],
    }

    first = await api_client.post("/orders", headers=headers, json=payload)
    await redis_client.flushdb()
    second = await api_client.post("/orders", headers=headers, json=payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
