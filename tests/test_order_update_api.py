"""HTTP-level tests for PATCH /orders/{order_id}: what changes, and what is refused."""

import uuid
from decimal import Decimal

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
async def test_patch_updates_the_delivery_date(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 2)])
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}",
        headers={"X-Entity-Id": str(entity.id)},
        json={"delivery_date": "2026-10-15"},
    )

    assert response.status_code == 200
    reread = await api_client.get(f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)})
    assert reread.json()["delivery_date"] == "2026-10-15"
    assert reread.json()["lines"][0]["quantity"] == 2


@pytest.mark.asyncio
async def test_patch_replaces_the_lines_at_the_current_master_price(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    item.list_price = Decimal("2500.00")
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}",
        headers={"X-Entity-Id": str(entity.id)},
        json={"lines": [{"item_id": str(item.id), "quantity": 4}]},
    )

    assert response.status_code == 200
    assert response.json()["lines"] == [
        {"item_id": str(item.id), "quantity": 4, "unit_price": "2500.00"}
    ]


@pytest.mark.asyncio
async def test_patch_on_a_confirmed_order_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(
        db_session, entity=entity, customer=customer, lines=[(item, 1)], status="CONFIRMED"
    )
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}",
        headers={"X-Entity-Id": str(entity.id)},
        json={"delivery_date": "2026-10-15"},
    )

    assert response.status_code == 409
    reread = await api_client.get(f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)})
    assert reread.json()["delivery_date"] == "2026-09-01"


@pytest.mark.asyncio
async def test_patch_with_an_unknown_id_returns_404(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, _, _ = await _seed_masters(db_session)
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{uuid.uuid4()}",
        headers={"X-Entity-Id": str(entity.id)},
        json={"delivery_date": "2026-10-15"},
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_of_another_entitys_order_returns_404(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    owner, customer, item = await _seed_masters(db_session)
    intruder = await make_sales_entity(db_session, code="ENT-02", name="Another Co.")
    order = await make_order(db_session, entity=owner, customer=customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}",
        headers={"X-Entity-Id": str(intruder.id)},
        json={"delivery_date": "2026-10-15"},
    )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_patch_with_duplicate_item_ids_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}",
        headers={"X-Entity-Id": str(entity.id)},
        json={
            "lines": [
                {"item_id": str(item.id), "quantity": 1},
                {"item_id": str(item.id), "quantity": 2},
            ]
        },
    )

    assert response.status_code == 422


@pytest.mark.parametrize("body", [{}, {"currency": "USD"}])
@pytest.mark.asyncio
async def test_patch_without_an_updatable_field_is_rejected(
    db_session: AsyncSession, api_client: AsyncClient, body: dict[str, str]
) -> None:
    entity, customer, item = await _seed_masters(db_session)
    order = await make_order(db_session, entity=entity, customer=customer, lines=[(item, 1)])
    await db_session.commit()

    response = await api_client.patch(
        f"/orders/{order.id}", headers={"X-Entity-Id": str(entity.id)}, json=body
    )

    assert response.status_code == 422
