"""The consumer loop, shared by both services (design §5.2, §5.7).

Both services consume: inventory-worker reads orders.events and order-api
reads inventory.events, so the loop is handed its group, its dedup table and
its handler instead of importing any of them. Nothing here is async on
purpose — the Kafka consumer blocks, exactly as the producer does.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.orm import Session


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
