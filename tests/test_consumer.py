"""The consume loop's judgement, with no broker running (design §7.7)."""

import json
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from fakes import FakeConsumer, FakeMessage
from sqlalchemy import select
from sqlalchemy.orm import Session

from common.consumer import ConsumerConfig, handle_message
from inventory_worker.models import Outbox, ProcessedEvent

CONFIG = ConsumerConfig(topic="orders.events", group_id="inventory-worker")


def _message(event_id: uuid.UUID) -> FakeMessage:
    """One envelope carrying only the fields the loop itself reads."""
    body = {
        "event_id": str(event_id),
        "event_type": "OrderConfirmed",
        "entity_id": str(uuid.uuid4()),
        "aggregate_id": str(uuid.uuid4()),
        "payload": {},
    }
    return FakeMessage(json.dumps(body).encode())


def _recording_handler(
    seen: list[dict[str, Any]],
) -> Callable[[Session, dict[str, Any]], None]:
    """A handler that both records the call and writes a row, as a real one does."""

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        seen.append(envelope)
        session.add(
            Outbox(
                entity_id=uuid.UUID(envelope["entity_id"]),
                aggregate_type="Order",
                aggregate_id=uuid.UUID(envelope["aggregate_id"]),
                event_type="InventoryReserved",
                payload={},
            )
        )

    return handler


@pytest.mark.asyncio
async def test_a_new_event_is_handled_and_its_offset_committed(
    sync_session: Session, sync_session_factory: Callable[[], Session]
) -> None:
    seen: list[dict[str, Any]] = []
    consumer = FakeConsumer()
    message = _message(uuid.uuid4())

    handle_message(
        message,
        session_factory=sync_session_factory,
        consumer=consumer,
        processed_events=ProcessedEvent,
        handler=_recording_handler(seen),
        config=CONFIG,
    )

    assert len(seen) == 1
    assert sync_session.scalars(select(ProcessedEvent)).one().consumer_name == "inventory-worker"
    assert len(sync_session.scalars(select(Outbox)).all()) == 1
    assert consumer.committed == [message]


@pytest.mark.asyncio
async def test_a_redelivered_event_is_skipped_but_its_offset_committed(
    sync_session: Session, sync_session_factory: Callable[[], Session]
) -> None:
    seen: list[dict[str, Any]] = []
    consumer = FakeConsumer()
    message = _message(uuid.uuid4())
    handler = _recording_handler(seen)
    for _ in range(2):
        handle_message(
            message,
            session_factory=sync_session_factory,
            consumer=consumer,
            processed_events=ProcessedEvent,
            handler=handler,
            config=CONFIG,
        )

    assert len(seen) == 1
    assert len(sync_session.scalars(select(Outbox)).all()) == 1
    assert len(consumer.committed) == 2


@pytest.mark.asyncio
async def test_a_failing_handler_leaves_the_offset_uncommitted(
    sync_session: Session, sync_session_factory: Callable[[], Session]
) -> None:
    def handler(session: Session, envelope: dict[str, Any]) -> None:
        raise RuntimeError("the database went away")

    consumer = FakeConsumer()

    with pytest.raises(RuntimeError):
        handle_message(
            _message(uuid.uuid4()),
            session_factory=sync_session_factory,
            consumer=consumer,
            processed_events=ProcessedEvent,
            handler=handler,
            config=CONFIG,
        )

    assert len(consumer.committed) == 0
    assert len(sync_session.scalars(select(ProcessedEvent)).all()) == 0
    assert len(sync_session.scalars(select(Outbox)).all()) == 0
