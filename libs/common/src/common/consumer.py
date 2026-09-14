"""The consumer loop, shared by both services (design §5.2, §5.7).

Both services consume: inventory-worker reads orders.events and order-api
reads inventory.events, so the loop is handed its group, its dedup table and
its handler instead of importing any of them. Nothing here is async on
purpose — the Kafka consumer blocks, exactly as the producer does.
"""

import json
import random
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import structlog
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from common.messaging import ProcessedEventMixin

logger = structlog.stdlib.get_logger(__name__)

# Header values go on the wire as bytes, and the client encodes a str for us.
# The type is spelled exactly as confluent-kafka spells it: list is invariant,
# so a narrower spelling here would make the real producer fail this module's
# protocol for no reason.
Headers = list[tuple[str, str | bytes | None]]


class PermanentFailure(Exception):
    """A failure that would fail identically on every attempt (design §5.7).

    Retrying it only spends the backoff schedule to arrive at the same
    outcome, so the retry loop lets it through untouched and the message goes
    to the dead-letter path on its first attempt.
    """


class Message(Protocol):
    """The slice of a consumed Kafka message this loop reads."""

    def value(self) -> bytes | None: ...

    def error(self) -> object | None: ...

    # Where this message sits in the log, and the key it was published with.
    # The dead-letter path copies the key so a re-injection lands on the same
    # partition, and reports the position so an operator can find the original
    # (design §5.6).
    def key(self) -> bytes | None: ...

    def topic(self) -> str | None: ...

    def partition(self) -> int | None: ...

    def offset(self) -> int | None: ...


class Consumer(Protocol):
    """The slice of the Kafka consumer API this loop uses."""

    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...

    # The message must be one this very consumer handed back, which no type here
    # can express: the real client accepts only its own Message class, while
    # poll() is declared to return this module's structural Message.
    def commit(self, *, message: Any, asynchronous: Literal[False]) -> object: ...

    def close(self) -> None: ...


class DeadLetterProducer(Protocol):
    """The slice of the Kafka producer API the dead-letter path uses.

    Declared here rather than imported, exactly as the loop's consumer is: the
    real client is assembled in common.kafka, and nothing in this module knows
    which library it came from (design §5.2).
    """

    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: Headers,
        on_delivery: Callable[[object | None, Any], None],
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


Handler = Callable[[Session, dict[str, Any]], None]


@dataclass(frozen=True)
class ConsumerConfig:
    """What one consuming process is: the topic it reads and the group it reads as.

    group_id doubles as the processed_events consumer_name (design §3.2): the
    dedup ledger keys on the logical consumer, never on the instance, or a
    redelivery to a different instance would not be recognised as a duplicate.
    poll_timeout is how long one poll waits before reporting that nothing
    arrived; it costs only idle latency, never a missed message.
    max_attempts and retry_base fix design §5.5's schedule: five attempts,
    with the wait ceiling doubling from retry_base. Lengthening either means
    re-checking max.poll.interval.ms, which the total must stay well below.
    """

    topic: str
    group_id: str
    poll_timeout: float = 1.0
    max_attempts: int = 5
    retry_base: float = 1.0
    dlq_flush_timeout: float = 10.0

    @property
    def dlq_topic(self) -> str:
        """This consumer's dead-letter topic: <topic>.<consumer>.dlq (design §4.2).

        Derived, not configured: the pair it names is already fixed by the two
        fields above, so a separate setting could only ever disagree with them.
        """
        return f"{self.topic}.{self.group_id}.dlq"


def _backoff(attempt: int, *, base: float) -> float:
    """How long to wait after a failed attempt: full jitter (design §5.5).

    The ceiling doubles per attempt — 1 s, 2 s, 4 s, 8 s from a 1 s base — and
    the wait is a random point below it, never the ceiling itself. Waiting the
    ceiling exactly would make every consumer of one recovering database retry
    in the same instant, which is the spike the jitter exists to break up.
    """
    return random.uniform(0, base * 2 ** (attempt - 1))


def _diagnostics(
    message: Message, *, config: ConsumerConfig, error: Exception, attempts: int
) -> Headers:
    """What an operator needs to classify a dead-lettered message (design §5.6).

    The diagnosis rides in headers so the value can stay byte-for-byte what was
    published: re-injection is then a plain republish with no unwrap step, and
    a message whose envelope could not even be parsed still arrives with a
    readable account of why it is here.
    """
    return [
        ("original_topic", str(message.topic())),
        ("original_partition", str(message.partition())),
        ("original_offset", str(message.offset())),
        ("error_class", type(error).__name__),
        ("error_message", str(error)),
        ("attempts", str(attempts)),
        ("failed_at", datetime.now(UTC).isoformat()),
        ("consumer", config.group_id),
    ]


def dead_letter(
    message: Message,
    *,
    consumer: Consumer,
    producer: DeadLetterProducer,
    config: ConsumerConfig,
    error: Exception,
    attempts: int,
) -> None:
    """Copy one message to the dead-letter topic, then let its offset move on (§5.6).

    A failed publish plus a committed offset would be the one outcome the
    design refuses — a message dropped in silence, with no redelivery left to
    recover it — so the commit waits for the broker's ack.
    """
    # The delivery report is the ack, and it fires while flush() runs, exactly
    # as the poller's does (design §5.4).
    delivered: list[object | None] = []
    producer.produce(
        config.dlq_topic,
        key=message.key(),
        value=message.value(),
        headers=_diagnostics(message, config=config, error=error, attempts=attempts),
        on_delivery=lambda delivery_error, _message: delivered.append(delivery_error),
    )
    while producer.flush(config.dlq_flush_timeout) > 0:
        # Every send resolves within message.timeout.ms, so this ends.
        continue

    # Anything but one clean ack means the copy is not stored anywhere. Raising
    # leaves the offset uncommitted, so a restart has the message redelivered
    # rather than lost (design §5.6).
    if delivered != [None]:
        raise RuntimeError(f"the dead-letter copy was not acked: {delivered}")

    # The copy is durable now, so say so before the offset moves: DLQ depth is
    # the operator's detection signal (design §5.6), and a silent hand-off
    # leaves the failure invisible until someone thinks to look.
    logger.error(
        "event_dead_lettered",
        dlq_topic=config.dlq_topic,
        consumer=config.group_id,
        attempts=attempts,
        error_class=type(error).__name__,
    )
    consumer.commit(message=message, asynchronous=False)


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
    # Decoding is judged here rather than in the retry loop: whether a failure
    # can be retried is a property of the failure, and only this step knows
    # that its own failures are permanent (design §5.7).
    value = message.value()
    if value is None:
        raise PermanentFailure("the message carries no value")
    try:
        envelope = json.loads(value)
    except json.JSONDecodeError as error:
        raise PermanentFailure(f"the message is not JSON: {error}") from error

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


def handle_with_retry(
    message: Message,
    *,
    session_factory: Callable[[], Session],
    consumer: Consumer,
    producer: DeadLetterProducer,
    processed_events: type[ProcessedEventMixin],
    handler: Handler,
    config: ConsumerConfig,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Process one message, retrying a technical failure in place (design §5.5).

    Retrying in place blocks the partition behind this message, which is the
    deliberate trade: a technical failure is usually environmental, so the
    messages waiting behind it would fail identically anyway, and blocking is
    what keeps one order's events in their order. The attempt count lives in
    this loop and nowhere else — persisting it would put the failure path back
    on the database, the component most likely to be failing (design §5.5).
    """
    for attempt in range(1, config.max_attempts + 1):
        # Each attempt runs the whole of handle_message, so a retry opens a
        # fresh session and starts from a transaction the last failure did not
        # dirty.
        try:
            handle_message(
                message,
                session_factory=session_factory,
                consumer=consumer,
                processed_events=processed_events,
                handler=handler,
                config=config,
            )
            return
        # A shortage never arrives here — a business failure is a return value,
        # not an exception (design §5.3). What arrives is permanent, which no
        # amount of waiting fixes, or technical, which is worth another try.
        except PermanentFailure as error:
            # Waiting cannot change a permanent failure's outcome, so it skips
            # the schedule entirely and leaves on its first attempt (§5.5).
            dead_letter(
                message,
                consumer=consumer,
                producer=producer,
                config=config,
                error=error,
                attempts=attempt,
            )
            return
        except Exception as error:
            # The schedule is spent. The message leaves through the dead-letter
            # path so the messages queued behind it can flow again (§5.6).
            if attempt == config.max_attempts:
                dead_letter(
                    message,
                    consumer=consumer,
                    producer=producer,
                    config=config,
                    error=error,
                    attempts=attempt,
                )
                return

            # A retry that says nothing is indistinguishable from a consumer
            # that has hung: these lines are the whole of what an operator sees
            # while the schedule runs (design §5.5).
            wait = _backoff(attempt, base=config.retry_base)
            logger.warning(
                "event_handle_failed",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                attempt=attempt,
                max_attempts=config.max_attempts,
                retry_in=round(wait, 3),
                error_class=type(error).__name__,
                error_message=str(error),
            )
            sleep(wait)


def run_forever(
    session_factory: Callable[[], Session],
    *,
    consumer: Consumer,
    producer: DeadLetterProducer,
    processed_events: type[ProcessedEventMixin],
    handler: Handler,
    config: ConsumerConfig,
    sleep: Callable[[float], None] = time.sleep,
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
            handle_with_retry(
                message,
                session_factory=session_factory,
                consumer=consumer,
                producer=producer,
                processed_events=processed_events,
                handler=handler,
                config=config,
                sleep=sleep,
            )
    finally:
        consumer.close()
