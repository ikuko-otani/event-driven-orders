"""HTTP-level tests for GET /orders/{order_id}: payload, 404, entity scoping."""

import uuid
from decimal import Decimal

import pytest
from factories import make_customer, make_item, make_order, make_sales_entity
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_get_order_returns_the_order_with_its_lines(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await make_sales_entity(db_session)
    customer = await make_customer(db_session, entity=entity)
    item = await make_item(db_session, list_price=Decimal("1234.50"))
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 3)])
    await db_session.commit()

    response = await api_client.get(f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)})

    assert response.status_code == 200
    body = response.json()
    assert body["order_number"] == order.order_number
    assert body["status"] == "PENDING"
    assert body["lines"] == [{"item_id": str(item.id), "quantity": 3, "unit_price": "1234.50"}]
    assert "idempotency_key" not in body


@pytest.mark.asyncio
async def test_get_order_with_an_unknown_id_returns_404(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity = await make_sales_entity(db_session)
    await db_session.commit()

    unknown_order_id = uuid.uuid4()
    response = await api_client.get(
        f"/orders/{unknown_order_id}", headers={"X-Entity-Id": str(entity.id)}
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_order_of_another_entity_returns_404(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    owner = await make_sales_entity(db_session)
    intruder = await make_sales_entity(db_session, code="ENT-02", name="Another Co.")
    customer = await make_customer(db_session, entity=owner)
    item = await make_item(db_session)
    order = await make_order(db_session, entity=owner, customer=customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.get(
        f"/orders/{order.id}", headers={"X-Entity-Id": str(intruder.id)}
    )

    assert response.status_code == 404
