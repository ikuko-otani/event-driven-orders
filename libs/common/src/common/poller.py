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
from typing import Any, Protocol, TypeVar

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from common.envelope import envelope
from common.messaging import OutboxMixin

OutboxRow = TypeVar("OutboxRow", bound=OutboxMixin)


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
