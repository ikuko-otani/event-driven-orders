"""Test doubles for infrastructure the unit tests deliberately do not start."""

from dataclasses import dataclass

from common.consumer import Message
from common.poller import DeliveryCallback


@dataclass(frozen=True)
class FakeDeliveryError:
    """A broker rejection whose permanence the test chooses (design §5.4)."""

    permanent: bool = False

    def retriable(self) -> bool:
        return not self.permanent


class FakeProducer:
    """A Kafka producer that records every send and reports the result on flush.

    Delivery reports arrive only while the real client is polled, so they fire
    in flush() here too: a test that forgets to flush sees nothing acked,
    exactly as production would. A message whose key appears in errors is
    rejected; every other one is acked.
    """

    def __init__(
        self,
        errors: dict[str, FakeDeliveryError] | None = None,
        *,
        flushes_needed: int = 1,
    ) -> None:
        self.errors: dict[str, FakeDeliveryError] = errors or {}
        self.messages: list[tuple[str, str, bytes]] = []
        self.flush_calls = 0
        self._flushes_needed = flushes_needed
        self._pending: list[tuple[str, DeliveryCallback]] = []

    def produce(self, topic: str, *, key: str, value: bytes, on_delivery: DeliveryCallback) -> None:
        self.messages.append((topic, key, value))
        self._pending.append((key, on_delivery))

    def flush(self, timeout: float) -> int:
        """Report what is still queued; only the last flush resolves the sends."""
        self.flush_calls += 1
        if self.flush_calls < self._flushes_needed:
            return len(self._pending)
        for key, callback in self._pending:
            callback(self.errors.get(key), None)
        self._pending.clear()
        return 0


@dataclass(frozen=True)
class FakeMessage:
    """One consumed message, carrying only the bytes the loop reads."""

    body: bytes

    def value(self) -> bytes | None:
        return self.body


class FakeConsumer:
    """A consumer that records whose offset was committed, and nothing else.

    It cannot fail a commit and it never rebalances; what it does model is the
    only thing the loop itself decides — whether the offset commit is reached.
    """

    def __init__(self) -> None:
        self.committed: list[Message] = []

    def commit(self, message: Message, *, asynchronous: bool) -> None:
        self.committed.append(message)
