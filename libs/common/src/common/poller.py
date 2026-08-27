"""The outbox poller loop, shared by both services (design §5.4).

Each service runs its own poller process over its own outbox table, so the
loop is given the topic and the Outbox model instead of importing either.
Nothing here is async on purpose: the Kafka producer blocks, so an event
loop would buy nothing.
"""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import func, select, update
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


def _on_delivery(row_id: uuid.UUID, acked: list[uuid.UUID]) -> DeliveryCallback:
    """Bind one row's id to its callback; the broker reports each send separately."""

    def callback(error: DeliveryError | None, _message: Any) -> None:
        if error is None:
            acked.append(row_id)

    return callback


def publish_batch(
    session: Session, *, outbox: type[OutboxMixin], producer: Producer, config: PollerConfig
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
    for row in rows:
        producer.produce(
            config.topic,
            key=str(row.aggregate_id),
            value=json.dumps(envelope(row)).encode(),
            on_delivery=_on_delivery(row.id, acked),
        )
    producer.flush(config.flush_timeout)

    if acked:
        session.execute(update(outbox).where(outbox.id.in_(acked)).values(published_at=func.now()))
    session.commit()
    return len(rows)
