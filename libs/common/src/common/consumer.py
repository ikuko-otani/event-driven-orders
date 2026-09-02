"""The consumer loop, shared by both services (design §5.2, §5.7).

Both services consume: inventory-worker reads orders.events and order-api
reads inventory.events, so the loop is handed its group, its dedup table and
its handler instead of importing any of them. Nothing here is async on
purpose — the Kafka consumer blocks, exactly as the producer does.
"""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from common.messaging import ProcessedEventMixin


class Message(Protocol):
    """The slice of a consumed Kafka message this loop reads."""

    def value(self) -> bytes | None: ...

    def error(self) -> object | None: ...


class Consumer(Protocol):
    """The slice of the Kafka consumer API this loop uses."""

    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...

    def commit(self, *, message: Message, asynchronous: Literal[False]) -> object: ...

    def close(self) -> None: ...


Handler = Callable[[Session, dict[str, Any]], None]


@dataclass(frozen=True)
class ConsumerConfig:
    """What one consuming process is: the topic it reads and the group it reads as.

    group_id doubles as the processed_events consumer_name (design §3.2): the
    dedup ledger keys on the logical consumer, never on the instance, or a
    redelivery to a different instance would not be recognised as a duplicate.
    poll_timeout is how long one poll waits before reporting that nothing
    arrived; it costs only idle latency, never a missed message.
    """

    topic: str
    group_id: str
    poll_timeout: float = 1.0


def _claim(
    session: Session,
    *,
    processed_events: type[ProcessedEventMixin],
    event_id: uuid.UUID,
    consumer_name: str,
) -> bool:
    """Take the event for this consumer, or report that someone already took it.

    ON CONFLICT DO NOTHING makes the check and the claim one statement, so two
    deliveries racing each other cannot both see "not processed yet" — the
    same argument the order INSERT uses as its idempotency claim (design §4.4).
    RETURNING, not rowcount, is what tells success from conflict: this
    driver/SQLAlchemy combination reports -1 (unknown) for a plain INSERT's
    rowcount, but a row comes back from RETURNING only when one was inserted.
    """
    result = session.execute(
        insert(processed_events)
        .values(event_id=event_id, consumer_name=consumer_name)
        .on_conflict_do_nothing()
        .returning(processed_events.event_id)
    )
    return result.first() is not None


def handle_message(
    message: Message,
    *,
    session_factory: Callable[[], Session],
    consumer: Consumer,
    processed_events: type[ProcessedEventMixin],
    handler: Handler,
    config: ConsumerConfig,
) -> None:
    """Process one message at most once, and only then commit its offset.

    The claim and the handler's writes share one transaction, so a crash
    mid-handler rolls the claim back too and the redelivery is processed
    normally. Committing the offset first would instead let Kafka consider a
    message consumed that no transaction ever recorded — at-most-once.
    """
    value = message.value()
    if value is None:
        raise ValueError("message carries no value")
    envelope = json.loads(value)

    with session_factory() as session:
        claimed = _claim(
            session,
            processed_events=processed_events,
            event_id=uuid.UUID(envelope["event_id"]),
            consumer_name=config.group_id,
        )
        if claimed:
            handler(session, envelope)
        session.commit()
    consumer.commit(message=message, asynchronous=False)


def run_forever(
    session_factory: Callable[[], Session],
    *,
    consumer: Consumer,
    processed_events: type[ProcessedEventMixin],
    handler: Handler,
    config: ConsumerConfig,
) -> None:
    """Consume the topic until something stops the process, one message at a time.

    A None from poll() is the ordinary idle case, not an error. Returning to
    poll() promptly is itself a requirement: a consumer that stays silent for
    longer than max.poll.interval.ms is evicted from its group and its
    partitions are handed to someone else (design §5.5).
    """
    consumer.subscribe([config.topic])
    try:
        while True:
            message = consumer.poll(config.poll_timeout)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                raise RuntimeError(f"the broker reported {error}")
            handle_message(
                message,
                session_factory=session_factory,
                consumer=consumer,
                processed_events=processed_events,
                handler=handler,
                config=config,
            )
    finally:
        consumer.close()
