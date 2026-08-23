"""HTTP-level tests for GET /orders: entity scoping, filters, ordering, paging."""

import pytest
from factories import make_customer, make_item, make_order, make_sales_entity
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.models import Customer, Item, SalesEntity


async def _seed_masters(session: AsyncSession) -> tuple[SalesEntity, Customer, Item]:
    entity = await make_sales_entity(session)
    customer = await make_customer(session, entity=entity)
    item = await make_item(session)
    return entity, customer, item


@pytest.mark.asyncio
async def test_order_list_returns_only_the_callers_entity(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    owner, customer, item = await _seed_masters(db_session)
    other = await make_sales_entity(db_session, code="ENT-02", name="Another Co.")
    other_customer = await make_customer(db_session, entity=other)
    mine = await make_order(db_session, entity=owner, customer=customer, lines=[(item, 1)])
    await make_order(db_session, entity=other, customer=other_customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.get("/orders", headers={"X-Entity-Id": str(owner.id)})

    assert response.status_code == 200
    assert [row["id"] for row in response.json()] == [str(mine.id)]


@pytest.mark.asyncio
async def test_order_list_filters_by_status(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    pending = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    await make_order(
        db_session,
        entity=entity,
        customer=customer,
        lines=[(item, 1)],
        status="CONFIRMED",
    )
    await db_session.commit()

    response = await api_client.get(
        "/orders", params={"status": "PENDING"}, headers={"X-Entity-Id": str(entity.id)}
    )

    assert [row["id"] for row in response.json()] == [str(pending.id)]


@pytest.mark.asyncio
async def test_order_list_filters_by_customer(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    other_customer = await make_customer(db_session, entity=entity, code="CUST-02")
    theirs = await make_order(db_session, entity=entity, customer=other_customer, lines=[(item, 1)])
    await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.get(
        "/orders",
        params={"customer_id": str(other_customer.id)},
        headers={"X-Entity-Id": str(entity.id)},
    )

    assert [row["id"] for row in response.json()] == [str(theirs.id)]


@pytest.mark.asyncio
async def test_order_list_is_newest_first_and_paginated(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    orders = [
        await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
        for _ in range(3)
    ]
    await db_session.commit()
    headers = {"X-Entity-Id": str(entity.id)}

    page1 = await api_client.get("/orders", params={"limit": 2}, headers=headers)
    page2 = await api_client.get("/orders", params={"limit": 2, "offset": 2}, headers=headers)

    assert [row["id"] for row in page1.json()] == [str(orders[2].id), str(orders[1].id)]
    assert [row["id"] for row in page2.json()] == [str(orders[0].id)]


@pytest.mark.asyncio
async def test_order_list_rejects_a_limit_above_the_maximum(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await make_sales_entity(db_session)
    await db_session.commit()

    response = await api_client.get(
        "/orders", params={"limit": 101}, headers={"X-Entity-Id": str(entity.id)}
    )

    assert response.status_code == 422
