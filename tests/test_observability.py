"""What an operator sees while the failure paths run (design §5.5, §5.6)."""

import json
import uuid

import structlog
from fakes import FakeConsumer, FakeMessage, FakeProducer
from sqlalchemy.orm import Session

from common.consumer import ConsumerConfig, handle_with_retry
from inventory_worker.models import ProcessedEvent

CONFIG = ConsumerConfig(topic="orders.events", group_id="inventory-worker")


def _unreachable_database() -> Session:
    """A session factory that fails the way a stopped database does.

    It raises before any statement runs, so the whole retry schedule can be
    exercised without a database to stop.
    """
    raise RuntimeError("the database went away")


def _message() -> FakeMessage:
    """One message carrying only the fields this path reads."""
    body = {"event_id": str(uuid.uuid4()), "event_type": "OrderConfirmed", "payload": {}}
    return FakeMessage(json.dumps(body).encode(), message_partition=7, message_offset=4242)


def test_every_failed_attempt_is_reported_before_the_message_is_dead_lettered() -> None:
    consumer = FakeConsumer()
    producer = FakeProducer()

    with structlog.testing.capture_logs() as logs:
        handle_with_retry(
            _message(),
            session_factory=_unreachable_database,
            consumer=consumer,
            producer=producer,
            processed_events=ProcessedEvent,
            handler=lambda session, event: None,
            config=CONFIG,
            sleep=lambda delay: None,
        )

    assert [entry["event"] for entry in logs] == ["event_handle_failed"] * 4 + [
        "event_dead_lettered"
    ]
    assert [entry["attempt"] for entry in logs[:4]] == [1, 2, 3, 4]
