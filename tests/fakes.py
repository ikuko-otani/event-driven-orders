"""Test doubles for infrastructure the unit tests deliberately do not start."""

from dataclasses import dataclass
from typing import Literal

from common.consumer import Headers, Message
from common.poller import DeliveryCallback


@dataclass(frozen=True)
class FakeDeliveryError:
    """A broker rejection carrying the code a real delivery report would carry.

    A field cannot share its name with the method that returns it, so the code
    is stored under a prefix, the same shape as FakeMessage's positions.
    """

    error_code: int

    def code(self) -> int:
        return self.error_code


class FakeProducer:
    """A Kafka producer that records every send and reports the result on flush.

    Delivery reports arrive only while the real client is polled, so they fire
    in flush() here too: a test that forgets to flush sees nothing acked,
    exactly as production would. A message whose key appears in errors is
    rejected; every other one is acked.
    """

    def __init__(
        self,
        errors: dict[str | bytes | None, FakeDeliveryError] | None = None,
        *,
        flushes_needed: int = 1,
    ) -> None:
        self.errors: dict[str | bytes | None, FakeDeliveryError] = errors or {}
        self.messages: list[tuple[str, str | bytes | None, bytes | None]] = []
        self.headers: list[Headers] = []
        self.flush_calls = 0
        self._flushes_needed = flushes_needed
        self._pending: list[tuple[str | bytes | None, DeliveryCallback]] = []

    def produce(
        self,
        topic: str,
        *,
        key: str | bytes | None,
        value: bytes | None,
        on_delivery: DeliveryCallback,
        headers: Headers | None = None,
    ) -> None:
        self.messages.append((topic, key, value))
        # Kept beside the message rather than inside it, so the poller's
        # three-part tuple keeps its shape; only the dead-letter path sends any.
        self.headers.append(headers or [])
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
    """One consumed message: the bytes the loop reads, and the error it checks."""

    body: bytes
    broker_error: str | None = None

    # A field cannot share its name with the method that returns it, so the
    # position fields carry a prefix. The defaults stand in for an ordinary
    # message; a test asserting on dead-letter headers sets them.
    message_key: bytes | None = None
    message_topic: str = "orders.events"
    message_partition: int = 0
    message_offset: int = 0

    def value(self) -> bytes | None:
        return self.body

    def error(self) -> object | None:
        return self.broker_error

    def key(self) -> bytes | None:
        return self.message_key

    def topic(self) -> str | None:
        return self.message_topic

    def partition(self) -> int | None:
        return self.message_partition

    def offset(self) -> int | None:
        return self.message_offset


class FakeConsumer:
    """A consumer that replays a script of polls and records what was committed.

    poll() hands back the scripted results in order, then None once the script
    is exhausted — the same "nothing to deliver" a real consumer reports on an
    idle timeout. It offers no way to stop the loop, because the real client
    offers none either: the loop ends when something raises.
    """

    def __init__(self, polls: list[Message | None] | None = None) -> None:
        self.committed: list[Message] = []
        self.subscribed: list[str] = []
        self.closed = False
        self._polls: list[Message | None] = list(polls or [])

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def poll(self, timeout: float) -> Message | None:
        return self._polls.pop(0) if self._polls else None

    def commit(self, *, message: Message, asynchronous: Literal[False]) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True
