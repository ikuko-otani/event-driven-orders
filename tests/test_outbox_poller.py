"""Unit tests for the outbox poller's publish cycle (design §5.4)."""

import json

import pytest
from factories import make_customer, make_item, make_order, make_sales_entity
from fakes import FakeDeliveryError, FakeProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.poller import PollerConfig, publish_batch
from order_api.events import order_confirmed_outbox
from order_api.models import Order, Outbox

CONFIG = PollerConfig(topic="orders.events")


async def _seed_confirmed_order(session: AsyncSession) -> Order:
    """One CONFIRMED order with the outbox row its confirm would have written."""
    entity = await make_sales_entity(session)
    customer = await make_customer(session, entity=entity)
    item = await make_item(session)
    order = await make_order(
        session, entity=entity, customer=customer, lines=[(item, 3)], status="CONFIRMED"
    )
    session.add(order_confirmed_outbox(order))
    await session.commit()
    return order


@pytest.mark.asyncio
async def test_publish_batch_sends_the_design_envelope_keyed_by_order_id(
    db_session: AsyncSession, sync_session: Session
) -> None:
    order = await _seed_confirmed_order(db_session)
    producer = FakeProducer()

    publish_batch(sync_session, outbox=Outbox, producer=producer, config=CONFIG)

    assert len(producer.messages) == 1
    topic, key, value = producer.messages[0]
    assert (topic, key) == ("orders.events", str(order.id))
    assert value is not None
    sent = json.loads(value)
    assert sent["event_type"] == "OrderConfirmed"
    assert sent["event_version"] == 1
    assert sent["aggregate_id"] == str(order.id)
    assert sent["payload"]["order_number"] == order.order_number


@pytest.mark.asyncio
async def test_an_acked_row_is_marked_published(
    db_session: AsyncSession, sync_session: Session
) -> None:
    await _seed_confirmed_order(db_session)

    publish_batch(sync_session, outbox=Outbox, producer=FakeProducer(), config=CONFIG)

    rows = list(sync_session.scalars(select(Outbox)))
    assert len(rows) == 1
    assert rows[0].published_at is not None


@pytest.mark.asyncio
async def test_a_published_row_is_not_sent_again_on_the_next_cycle(
    db_session: AsyncSession, sync_session: Session
) -> None:
    await _seed_confirmed_order(db_session)
    publish_batch(sync_session, outbox=Outbox, producer=FakeProducer(), config=CONFIG)

    second = FakeProducer()
    selected = publish_batch(sync_session, outbox=Outbox, producer=second, config=CONFIG)

    assert selected == 0
    assert second.messages == []


@pytest.mark.asyncio
async def test_a_transient_send_failure_is_retried_on_the_next_cycle(
    db_session: AsyncSession, sync_session: Session
) -> None:
    order = await _seed_confirmed_order(db_session)
    rejecting = FakeProducer({str(order.id): FakeDeliveryError()})

    publish_batch(sync_session, outbox=Outbox, producer=rejecting, config=CONFIG)

    row = sync_session.scalars(select(Outbox)).one()
    assert (row.published_at, row.quarantined_at) == (None, None)
    assert row.publish_attempts == 1

    retried = FakeProducer()
    publish_batch(sync_session, outbox=Outbox, producer=retried, config=CONFIG)

    assert len(retried.messages) == 1


@pytest.mark.asyncio
async def test_a_permanent_send_failure_is_quarantined_and_never_sent_again(
    db_session: AsyncSession, sync_session: Session
) -> None:
    order = await _seed_confirmed_order(db_session)
    rejecting = FakeProducer({str(order.id): FakeDeliveryError(permanent=True)})

    publish_batch(sync_session, outbox=Outbox, producer=rejecting, config=CONFIG)

    row = sync_session.scalars(select(Outbox)).one()
    assert row.published_at is None
    assert row.quarantined_at is not None

    next_cycle = FakeProducer()
    selected = publish_batch(sync_session, outbox=Outbox, producer=next_cycle, config=CONFIG)

    assert selected == 0
    assert next_cycle.messages == []


@pytest.mark.asyncio
async def test_a_transient_failure_at_the_attempt_limit_is_quarantined(
    db_session: AsyncSession, sync_session: Session
) -> None:
    order = await _seed_confirmed_order(db_session)
    config = PollerConfig(topic="orders.events", max_attempts=1)
    rejecting = FakeProducer({str(order.id): FakeDeliveryError()})

    publish_batch(sync_session, outbox=Outbox, producer=rejecting, config=config)

    row = sync_session.scalars(select(Outbox)).one()
    assert row.publish_attempts == 1
    assert row.quarantined_at is not None


@pytest.mark.asyncio
async def test_publish_batch_flushes_until_the_client_queue_is_empty(
    db_session: AsyncSession, sync_session: Session
) -> None:
    await _seed_confirmed_order(db_session)
    slow = FakeProducer(flushes_needed=2)

    publish_batch(sync_session, outbox=Outbox, producer=slow, config=CONFIG)

    assert slow.flush_calls == 2
    assert sync_session.scalars(select(Outbox)).one().published_at is not None
