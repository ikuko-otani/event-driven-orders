"""The consumer against a real broker: the wiring no fake can check (design §5.2)."""

import json
import time
import uuid
from collections.abc import Callable
from contextlib import closing
from typing import Any

import pytest
from confluent_kafka import Consumer, Message, TopicPartition
from sqlalchemy import select
from sqlalchemy.orm import Session

from common.consumer import ConsumerConfig, handle_message, handle_with_retry
from common.kafka import build_consumer, build_producer
from common.settings import KafkaSettings
from inventory_worker.models import ProcessedEvent


def _publish(topic: str, event_id: uuid.UUID) -> None:
    """Put one OrderConfirmed envelope on the topic, the way the poller does."""
    aggregate_id = uuid.uuid4()
    envelope: dict[str, Any] = {
        "event_id": str(event_id),
        "event_type": "OrderConfirmed",
        "entity_id": str(uuid.uuid4()),
        "aggregate_id": str(aggregate_id),
        "payload": {},
    }
    producer = build_producer(KafkaSettings())
    producer.produce(topic, key=str(aggregate_id), value=json.dumps(envelope).encode())
    producer.flush(10.0)


def _consume_one(consumer: Consumer, *, timeout: float = 20.0) -> Message:
    """Wait for one message, failing the test instead of blocking forever."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is not None:
            assert message.error() is None, message.error()
            return message
    pytest.fail("the broker delivered no message before the timeout")


@pytest.mark.asyncio
async def test_an_event_from_a_real_broker_is_claimed_and_its_offset_advanced(
    sync_session: Session,
    sync_session_factory: Callable[[], Session],
    kafka_topic: str,
) -> None:
    config = ConsumerConfig(topic=kafka_topic, group_id=f"inventory-worker-{uuid.uuid4()}")
    event_id = uuid.uuid4()
    seen: list[dict[str, Any]] = []

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        seen.append(envelope)

    with closing(build_consumer(KafkaSettings(), config)) as consumer:
        consumer.subscribe([config.topic])
        _publish(config.topic, event_id)
        message = _consume_one(consumer)
        handle_message(
            message,
            session_factory=sync_session_factory,
            consumer=consumer,
            processed_events=ProcessedEvent,
            handler=handler,
            config=config,
        )
        partition, offset = message.partition(), message.offset()
        assert partition is not None and offset is not None
        committed = consumer.committed([TopicPartition(config.topic, partition)], timeout=10.0)

    assert [envelope["event_id"] for envelope in seen] == [str(event_id)]
    assert sync_session.scalars(select(ProcessedEvent)).one().event_id == event_id
    assert committed[0].offset == offset + 1


@pytest.mark.asyncio
async def test_a_handler_that_keeps_failing_puts_the_message_on_the_real_dlq_topic(
    sync_session_factory: Callable[[], Session],
    kafka_topic: str,
    make_topic: Callable[[str], str],
    kafka_consumer: Consumer,
) -> None:
    config = ConsumerConfig(topic=kafka_topic, group_id=f"inventory-worker-{uuid.uuid4()}")
    make_topic(config.dlq_topic)
    kafka_consumer.subscribe([config.dlq_topic])

    def handler(session: Session, envelope: dict[str, Any]) -> None:
        raise RuntimeError("the database is unreachable")

    with closing(build_consumer(KafkaSettings(), config)) as consumer:
        consumer.subscribe([config.topic])
        _publish(config.topic, uuid.uuid4())
        message = _consume_one(consumer)
        # A real time.sleep would spend the whole §5.5 schedule, ~15 s of it.
        handle_with_retry(
            message,
            session_factory=sync_session_factory,
            consumer=consumer,
            producer=build_producer(KafkaSettings()),
            processed_events=ProcessedEvent,
            handler=handler,
            config=config,
            sleep=lambda _: None,
        )
        # Read the original off the message while its consumer is still open.
        original, key = message.value(), message.key()
        partition, offset = message.partition(), message.offset()
        assert partition is not None and offset is not None
        committed = consumer.committed([TopicPartition(config.topic, partition)], timeout=10.0)

    dead_lettered = _consume_one(kafka_consumer)
    raw_headers = dead_lettered.headers()
    assert raw_headers is not None
    diagnostics = dict(raw_headers)

    assert (dead_lettered.value(), dead_lettered.key()) == (original, key)
    assert diagnostics["original_topic"] == config.topic.encode()
    assert diagnostics["original_partition"] == str(partition).encode()
    assert diagnostics["original_offset"] == str(offset).encode()
    assert diagnostics["error_class"] == b"RuntimeError"
    assert diagnostics["attempts"] == b"5"
    assert committed[0].offset == offset + 1
