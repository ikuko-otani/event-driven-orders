"""HTTP-level tests for POST /orders/{order_id}/confirm: the transition and its outbox row."""

from uuid import uuid4

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


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CONFIRMED", "RESERVED", "RESERVATION_FAILED"])
async def test_confirming_a_non_pending_order_is_a_read_only_no_op(
    db_session: AsyncSession, api_client: AsyncClient, status: str
) -> None:
    """Every cell but PENDING answers 200, writes no outbox row, and changes nothing."""
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(
        db_session, entity=entity, customer=customer, lines=[(item, 2)], status=status
    )
    await db_session.commit()
    headers = {"X-Entity-Id": str(entity.id)}

    response = await api_client.post(f"/orders/{order.id}/confirm", headers=headers)

    assert response.status_code == 200
    assert response.json()["status"] == status
    reread = await api_client.get(f"/orders/{order.id}", headers=headers)
    assert reread.json()["status"] == status
    assert await _outbox_rows(db_session, order) == []


@pytest.mark.asyncio
async def test_confirming_an_absent_order_is_404(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, _, _ = await _seed_masters(db_session)
    await db_session.commit()

    response = await api_client.post(
        f"/orders/{uuid4()}/confirm", headers={"X-Entity-Id": str(entity.id)}
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_confirming_another_entitys_order_is_404_and_leaves_it_untouched(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    """A 403 would confirm the id exists; the order must also survive the attempt unchanged."""
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    other = await make_sales_entity(db_session, code="ENT-02", name="Other Trading Co.")
    await db_session.commit()

    response = await api_client.post(
        f"/orders/{order.id}/confirm", headers={"X-Entity-Id": str(other.id)}
    )

    assert response.status_code == 404
    reread = await api_client.get(f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)})
    assert reread.json()["status"] == "PENDING"
    assert await _outbox_rows(db_session, order) == []
