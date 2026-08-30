"""The outbox poller loop, shared by both services (design §5.4).

Each service runs its own poller process over its own outbox table, so the
loop is given the topic and the Outbox model instead of importing either.
Nothing here is async on purpose: the Kafka producer blocks, so an event
loop would buy nothing.
"""

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import case, func, select, update
from sqlalchemy.orm import Session

from common.envelope import envelope
from common.messaging import OutboxMixin


class DeliveryError(Protocol):
    """The error object the broker hands back to a delivery callback."""

    def retriable(self) -> bool: ...


DeliveryCallback = Callable[[DeliveryError | None, Any], None]


class Producer(Protocol):
    """The slice of the Kafka producer API this loop uses."""

    def produce(
        self, topic: str, *, key: str, value: bytes, on_delivery: DeliveryCallback
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class PollerConfig:
    """Design §5.4's tunables, externalized so a load test can re-tune them."""

    topic: str
    batch_size: int = 100
    poll_interval: float = 0.1
    flush_timeout: float = 10.0
    max_attempts: int = 5


def _on_delivery(
    row_id: uuid.UUID,
    acked: list[uuid.UUID],
    failed: list[tuple[uuid.UUID, DeliveryError]],
) -> DeliveryCallback:
    """Bind one row's id to its callback; the broker reports each send separately."""

    def callback(error: DeliveryError | None, _message: Any) -> None:
        if error is None:
            acked.append(row_id)
        else:
            failed.append((row_id, error))

    return callback


def publish_batch(
    session: Session,
    *,
    outbox: type[OutboxMixin],
    producer: Producer,
    config: PollerConfig,
) -> int:
    """Publish one batch of unpublished rows, then mark only the ones acked.

    Publish before mark: a crash between the two re-publishes those rows on
    restart, and the duplicate is absorbed downstream by processed_events.
    Marking first would instead lose every row marked but never sent.
    """
    rows = list(
        session.scalars(
            select(outbox)
            .where(outbox.published_at.is_(None), outbox.quarantined_at.is_(None))
            .order_by(outbox.created_at)
            .limit(config.batch_size)
        )
    )

    acked: list[uuid.UUID] = []
    failed: list[tuple[uuid.UUID, DeliveryError]] = []
    for row in rows:
        producer.produce(
            config.topic,
            key=str(row.aggregate_id),
            value=json.dumps(envelope(row)).encode(),
            on_delivery=_on_delivery(row.id, acked, failed),
        )
    while producer.flush(config.flush_timeout) > 0:
        # Every message resolves within message.timeout.ms, so this ends.
        continue

    if acked:
        session.execute(update(outbox).where(outbox.id.in_(acked)).values(published_at=func.now()))
    if failed:
        permanent = [row_id for row_id, error in failed if not error.retriable()]
        session.execute(
            update(outbox)
            .where(outbox.id.in_([row_id for row_id, _ in failed]))
            .values(
                publish_attempts=outbox.publish_attempts + 1,
                quarantined_at=case(
                    (
                        outbox.id.in_(permanent)
                        | (outbox.publish_attempts + 1 >= config.max_attempts),
                        func.now(),
                    ),
                    else_=None,
                ),
            )
        )
    session.commit()
    return len(rows)


def run_forever(
    session_factory: Callable[[], Session],
    *,
    outbox: type[OutboxMixin],
    producer: Producer,
    config: PollerConfig,
) -> None:
    """Drain the outbox forever, sleeping only when the last batch ran short.

    A full batch means a backlog is still draining, so the next cycle starts
    at once; a short one means the table is caught up, and polling harder
    would only add load without lowering latency.
    """
    while True:
        with session_factory() as session:
            selected = publish_batch(session, outbox=outbox, producer=producer, config=config)
        if selected < config.batch_size:
            time.sleep(config.poll_interval)
