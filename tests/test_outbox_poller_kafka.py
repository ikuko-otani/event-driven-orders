"""The poller against a real broker: the wiring no fake can check (design §5.4)."""

import json
import time

import pytest
from confluent_kafka import Consumer, Message
from factories import make_customer, make_item, make_order, make_sales_entity
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.kafka import PERMANENT_DELIVERY_ERRORS, build_producer
from common.poller import PollerConfig, publish_batch
from common.settings import KafkaSettings
from order_api.events import order_confirmed_outbox
from order_api.models import Outbox


def _consume_one(consumer: Consumer, *, timeout: float = 15.0) -> Message:
    """Wait for one message, failing the test instead of blocking forever."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is not None:
            assert message.error() is None, message.error()
            return message
    pytest.fail("the broker delivered no message before the timeout")


@pytest.mark.asyncio
async def test_a_confirmed_order_reaches_the_broker_and_is_marked_published(
    db_session: AsyncSession,
    sync_session: Session,
    kafka_topic: str,
    kafka_consumer: Consumer,
) -> None:
    entity = await make_sales_entity(db_session)
    customer = await make_customer(db_session, entity=entity)
    item = await make_item(db_session)
    order = await make_order(
        db_session,
        entity=entity,
        customer=customer,
        lines=[(item, 3)],
        status="CONFIRMED",
    )
    db_session.add(order_confirmed_outbox(order))
    await db_session.commit()

    kafka_consumer.subscribe([kafka_topic])
    publish_batch(
        sync_session,
        outbox=Outbox,
        producer=build_producer(KafkaSettings()),
        config=PollerConfig(topic=kafka_topic),
    )

    message = _consume_one(kafka_consumer)
    assert message.key() == str(order.id).encode()
    value = message.value()
    assert value is not None
    assert json.loads(value)["event_type"] == "OrderConfirmed"
    assert sync_session.scalars(select(Outbox)).one().published_at is not None


@pytest.mark.asyncio
async def test_a_send_to_an_unreachable_broker_is_left_for_the_next_cycle(
    db_session: AsyncSession, sync_session: Session
) -> None:
    """The failure only a real client can report: the code in its delivery error.

    Nothing listens on the address, so every send times out — the outage this
    guards against, and the case where the fake and the real client used to
    disagree (design §5.4).
    """
    entity = await make_sales_entity(db_session)
    customer = await make_customer(db_session, entity=entity)
    item = await make_item(db_session)
    order = await make_order(
        db_session, entity=entity, customer=customer, lines=[(item, 3)], status="CONFIRMED"
    )
    db_session.add(order_confirmed_outbox(order))
    await db_session.commit()

    # A timeout short enough for a test, against an address nothing answers on.
    unreachable = build_producer(
        KafkaSettings(bootstrap_servers="127.0.0.1:1", message_timeout_ms=2000)
    )
    publish_batch(
        sync_session,
        outbox=Outbox,
        producer=unreachable,
        config=PollerConfig(topic="orders.events", permanent_errors=PERMANENT_DELIVERY_ERRORS),
    )

    # Not quarantined: one outage must not take the row out of the poller's
    # sight, it must only spend one of its attempts (design §5.4).
    row = sync_session.scalars(select(Outbox)).one()
    assert (row.published_at, row.quarantined_at) == (None, None)
    assert row.publish_attempts == 1
