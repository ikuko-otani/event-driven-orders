"""How order-api applies what inventory-worker sends back (design §5.7)."""

import json
import logging
import uuid
from collections.abc import Callable

import pytest
from factories import make_inventory_reply_event
from fakes import FakeConsumer, FakeMessage
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from common.consumer import ConsumerConfig, handle_message
from order_api.handlers import handle_inventory_event
from order_api.models import Order, ProcessedEvent

CONFIG = ConsumerConfig(topic="inventory.events", group_id="order-api")


@pytest.mark.asyncio
async def test_a_reservation_moves_the_order_to_reserved(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await make_inventory_reply_event(db_session, sync_session)

    handle_inventory_event(sync_session, event)
    sync_session.commit()

    assert sync_session.scalars(select(Order)).one().status == "RESERVED"


@pytest.mark.asyncio
async def test_an_event_for_an_unknown_order_changes_nothing_and_warns(
    db_session: AsyncSession, sync_session: Session, caplog: pytest.LogCaptureFixture
) -> None:
    event = await make_inventory_reply_event(db_session, sync_session)
    payload = {**event["payload"], "order_id": str(uuid.uuid4())}

    with caplog.at_level(logging.WARNING):
        handle_inventory_event(sync_session, {**event, "payload": payload})
    sync_session.commit()

    assert sync_session.scalars(select(Order)).one().status == "CONFIRMED"
    assert "order_state_mismatch" in caplog.text


@pytest.mark.asyncio
async def test_a_redelivered_reservation_is_applied_once(
    db_session: AsyncSession,
    sync_session: Session,
    sync_session_factory: Callable[[], Session],
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = await make_inventory_reply_event(db_session, sync_session)
    message = FakeMessage(json.dumps(event).encode())
    consumer = FakeConsumer()

    # The same message twice, through the loop rather than the handler: the
    # dedup claim lives there, so calling the handler directly would not test it.
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            handle_message(
                message,
                session_factory=sync_session_factory,
                consumer=consumer,
                processed_events=ProcessedEvent,
                handler=handle_inventory_event,
                config=CONFIG,
            )

    assert sync_session.scalars(select(Order)).one().status == "RESERVED"
    assert len(list(sync_session.scalars(select(ProcessedEvent)))) == 1
    assert "order_state_mismatch" not in caplog.text


@pytest.mark.asyncio
async def test_a_shortage_moves_the_order_to_reservation_failed(
    db_session: AsyncSession, sync_session: Session
) -> None:
    event = await make_inventory_reply_event(db_session, sync_session, lines=((1, 5),))

    handle_inventory_event(sync_session, event)
    sync_session.commit()

    assert event["event_type"] == "InventoryReservationFailed"
    assert sync_session.scalars(select(Order)).one().status == "RESERVATION_FAILED"


@pytest.mark.asyncio
async def test_a_redelivered_shortage_compensates_once(
    db_session: AsyncSession,
    sync_session: Session,
    sync_session_factory: Callable[[], Session],
) -> None:
    event = await make_inventory_reply_event(db_session, sync_session, lines=((1, 5),))
    message = FakeMessage(json.dumps(event).encode())
    consumer = FakeConsumer()

    # Through the loop rather than the handler, and twice: the compensation and
    # the claim that absorbs the redelivery commit in one transaction (§5.3).
    for _ in range(2):
        handle_message(
            message,
            session_factory=sync_session_factory,
            consumer=consumer,
            processed_events=ProcessedEvent,
            handler=handle_inventory_event,
            config=CONFIG,
        )

    assert sync_session.scalars(select(Order)).one().status == "RESERVATION_FAILED"
    assert len(list(sync_session.scalars(select(ProcessedEvent)))) == 1
