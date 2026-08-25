"""HTTP-level tests for POST /orders/{order_id}/confirm: the transition and its outbox row."""

import pytest
from factories import make_customer, make_item, make_order, make_sales_entity
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from order_api.models import Customer, Item, Order, Outbox, SalesEntity


async def _seed_masters(session: AsyncSession) -> tuple[SalesEntity, Customer, Item]:
    entity = await make_sales_entity(session)
    customer = await make_customer(session, entity=entity)
    item = await make_item(session)
    return entity, customer, item


async def _outbox_rows(session: AsyncSession, order: Order) -> list[Outbox]:
    result = await session.execute(select(Outbox).where(Outbox.aggregate_id == order.id))
    return list(result.scalars())


@pytest.mark.asyncio
async def test_confirm_moves_a_pending_order_to_confirmed_and_writes_one_outbox_row(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 3)])
    await db_session.commit()

    response = await api_client.post(
        f"/orders/{order.id}/confirm", headers={"X-Entity-Id": str(entity.id)}
    )

    assert response.status_code == 200
    reread = await api_client.get(f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)})
    assert reread.json()["status"] == "CONFIRMED"

    rows = await _outbox_rows(db_session, order)
    assert len(rows) == 1
    assert rows[0].event_type == "OrderConfirmed"
    assert rows[0].published_at is None
    assert rows[0].payload["lines"] == [{"item_id": str(item.id), "quantity": 3}]


@pytest.mark.asyncio
async def test_confirming_twice_does_not_write_a_second_outbox_row(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    await db_session.commit()
    headers = {"X-Entity-Id": str(entity.id)}

    first = await api_client.post(f"/orders/{order.id}/confirm", headers=headers)
    second = await api_client.post(f"/orders/{order.id}/confirm", headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "CONFIRMED"
    assert len(await _outbox_rows(db_session, order)) == 1
