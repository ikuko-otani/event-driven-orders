"""What an operator sees while the failure paths run (design §5.5, §5.6)."""

import json
import uuid

import structlog
from fakes import FakeConsumer, FakeMessage, FakeProducer
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy.orm import Session

from common.consumer import ConsumerConfig, handle_with_retry
from common.observability import _stamp_trace
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


def test_the_dead_letter_line_names_the_message_it_set_aside() -> None:
    message = _message()

    with structlog.testing.capture_logs() as logs:
        handle_with_retry(
            message,
            session_factory=_unreachable_database,
            consumer=FakeConsumer(),
            producer=FakeProducer(),
            processed_events=ProcessedEvent,
            handler=lambda session, event: None,
            config=CONFIG,
            sleep=lambda delay: None,
        )

    line = logs[-1]
    assert (line["topic"], line["partition"], line["offset"]) == ("orders.events", 7, 4242)
    assert line["event_id"] == json.loads(message.body)["event_id"]


def test_a_message_that_never_parsed_is_still_named_by_its_position() -> None:
    message = FakeMessage(b"not an envelope", message_partition=3, message_offset=99)

    with structlog.testing.capture_logs() as logs:
        handle_with_retry(
            message,
            session_factory=_unreachable_database,
            consumer=FakeConsumer(),
            producer=FakeProducer(),
            processed_events=ProcessedEvent,
            handler=lambda session, event: None,
            config=CONFIG,
            sleep=lambda delay: None,
        )

    [line] = logs
    assert (line["partition"], line["offset"]) == (3, 99)
    assert line["event_id"] is None
    assert line["error_class"] == "PermanentFailure"


def test_a_line_written_inside_a_span_carries_the_trace_it_belongs_to() -> None:
    # A provider of its own, never the global one: the span only has to be real
    # enough to have a context, and nothing here should reach a collector.
    tracer = TracerProvider().get_tracer(__name__)

    with tracer.start_as_current_span("order.confirm"):
        inside = _stamp_trace(None, "info", {"event": "order_confirmed"})
    # The same call once the span has closed, which is the half that catches a
    # trace id left behind by a binding.
    outside = _stamp_trace(None, "info", {"event": "order_confirmed"})

    assert len(inside["trace_id"]) == 32
    assert "trace_id" not in outside
