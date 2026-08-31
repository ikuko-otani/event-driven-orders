"""The consumer loop, shared by both services (design §5.2, §5.7).

Both services consume: inventory-worker reads orders.events and order-api
reads inventory.events, so the loop is handed its group, its dedup table and
its handler instead of importing any of them. Nothing here is async on
purpose — the Kafka consumer blocks, exactly as the producer does.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from common.messaging import ProcessedEventMixin


class Message(Protocol):
    """The slice of a consumed Kafka message this loop reads."""

    def value(self) -> bytes | None: ...


class Consumer(Protocol):
    """The slice of the Kafka consumer API this loop uses."""

    def commit(self, message: Message, *, asynchronous: bool) -> None: ...


Handler = Callable[[Session, dict[str, Any]], None]


@dataclass(frozen=True)
class ConsumerConfig:
    """What one consuming process is: the topic it reads and the group it reads as.

    group_id doubles as the processed_events consumer_name (design §3.2): the
    dedup ledger keys on the logical consumer, never on the instance, or a
    redelivery to a different instance would not be recognised as a duplicate.
    """

    topic: str
    group_id: str


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
    """
    result = session.execute(
        insert(processed_events)
        .values(event_id=event_id, consumer_name=consumer_name)
        .on_conflict_do_nothing()
    )
    return cast(CursorResult[Any], result).rowcount == 1
