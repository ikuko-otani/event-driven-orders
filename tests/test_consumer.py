"""The consume loop's judgement, with no broker running (design §7.7)."""

import json
import uuid
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest
from fakes import FakeConsumer, FakeMessage, FakeProducer
from sqlalchemy import select
from sqlalchemy.orm import Session

from common.consumer import (
    ConsumerConfig,
    handle_message,
    handle_with_retry,
    run_forever,
)
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


def _positioned(event_id: uuid.UUID) -> FakeMessage:
    """One message that states where in the log it sat, and under which key.

    The dead-letter headers are read back out of these fields, so leaving them
    at their defaults would prove nothing: partition 0, offset 0 reads exactly
    like a diagnosis that never filled them in.
    """
    return replace(
        _message(event_id),
        message_key=b"an-order-id",
        message_partition=7,
        message_offset=4242,
    )


def _failing_handler(session: Session, envelope: dict[str, Any]) -> None:
    """A handler whose failure is technical, so the whole schedule runs before the DLQ."""
    raise RuntimeError("the database went away")


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


@pytest.mark.asyncio
async def test_the_loop_dead_letters_a_failing_message_and_keeps_going(
    sync_session: Session, sync_session_factory: Callable[[], Session]
) -> None:
    """Handle the first message, then fail — the message behind it must still flow."""
    seen: list[dict[str, Any]] = []
    record = _recording_handler(seen)

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        """Handle the first message, then fail — the only way run_forever ends."""
        if seen:
            raise RuntimeError("the database went away")
        record(session, envelope)

    first = _message(uuid.uuid4())
    poisoned = _message(uuid.uuid4())
    consumer = FakeConsumer(
        [first, poisoned, FakeMessage(b"", broker_error="all brokers are down")]
    )
    producer = FakeProducer()

    with pytest.raises(RuntimeError):
        run_forever(
            sync_session_factory,
            consumer=consumer,
            producer=producer,
            processed_events=ProcessedEvent,
            handler=handler,
            config=CONFIG,
            sleep=lambda _: None,
        )

    assert len(seen) == 1
    assert [topic for topic, _, _ in producer.messages] == ["orders.events.inventory-worker.dlq"]
    assert consumer.committed == [first, poisoned]
    assert consumer.closed


@pytest.mark.asyncio
async def test_a_broker_error_ends_the_loop_without_committing_the_offset(
    sync_session_factory: Callable[[], Session],
) -> None:
    consumer = FakeConsumer([FakeMessage(b"", broker_error="all brokers are down")])
    producer = FakeProducer()

    with pytest.raises(RuntimeError):
        run_forever(
            sync_session_factory,
            consumer=consumer,
            producer=producer,
            processed_events=ProcessedEvent,
            handler=_recording_handler([]),
            config=CONFIG,
        )

    assert consumer.committed == []
    assert consumer.closed


@pytest.mark.asyncio
async def test_a_technical_failure_is_retried_five_times_then_dead_lettered(
    sync_session_factory: Callable[[], Session],
) -> None:
    attempts = 0
    slept: list[float] = []
    consumer = FakeConsumer()

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("the database went away")

    producer = FakeProducer()
    message = _message(uuid.uuid4())
    handle_with_retry(
        message,
        session_factory=sync_session_factory,
        consumer=consumer,
        producer=producer,
        processed_events=ProcessedEvent,
        handler=handler,
        config=CONFIG,
        sleep=slept.append,
    )

    assert attempts == 5
    assert all(0 <= delay <= ceiling for delay, ceiling in zip(slept, [1, 2, 4, 8], strict=True))
    assert producer.messages[0][0] == "orders.events.inventory-worker.dlq"
    assert consumer.committed == [message]


@pytest.mark.asyncio
async def test_a_failure_that_recovers_is_retried_until_it_succeeds(
    sync_session: Session, sync_session_factory: Callable[[], Session]
) -> None:
    attempts = 0
    slept: list[float] = []
    consumer = FakeConsumer()
    producer = FakeProducer()
    message = _message(uuid.uuid4())

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("the database is still coming back")

    handle_with_retry(
        message,
        session_factory=sync_session_factory,
        consumer=consumer,
        producer=producer,
        processed_events=ProcessedEvent,
        handler=handler,
        config=CONFIG,
        sleep=slept.append,
    )

    assert attempts == 3
    assert len(slept) == 2
    assert consumer.committed == [message]


@pytest.mark.asyncio
async def test_a_message_that_is_not_json_goes_straight_to_the_dead_letter_topic(
    sync_session_factory: Callable[[], Session],
) -> None:
    slept: list[float] = []
    consumer = FakeConsumer()

    producer = FakeProducer()
    message = FakeMessage(b"this is not an envelope")
    handle_with_retry(
        message,
        session_factory=sync_session_factory,
        consumer=consumer,
        producer=producer,
        processed_events=ProcessedEvent,
        handler=_recording_handler([]),
        config=CONFIG,
        sleep=slept.append,
    )

    assert slept == []
    assert producer.messages[0][0] == "orders.events.inventory-worker.dlq"
    assert consumer.committed == [message]


@pytest.mark.asyncio
async def test_the_dead_lettered_copy_carries_the_original_bytes_and_key(
    sync_session_factory: Callable[[], Session],
) -> None:
    consumer = FakeConsumer()
    producer = FakeProducer()
    message = _positioned(uuid.uuid4())

    handle_with_retry(
        message,
        session_factory=sync_session_factory,
        consumer=consumer,
        producer=producer,
        processed_events=ProcessedEvent,
        handler=_failing_handler,
        config=CONFIG,
        sleep=lambda _: None,
    )

    topic, key, value = producer.messages[0]
    assert topic == CONFIG.dlq_topic
    assert value == message.body
    assert key == message.message_key
