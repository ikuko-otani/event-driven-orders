"""Test doubles for infrastructure the unit tests deliberately do not start."""

from common.poller import DeliveryCallback


class FakeProducer:
    """A Kafka producer that records every send and acks them all on flush.

    Delivery reports arrive only while the real client is polled, so they fire
    in flush() here too: a test that forgets to flush sees nothing acked,
    exactly as production would.
    """

    def __init__(self) -> None:
        self.messages: list[tuple[str, str, bytes]] = []
        self._pending: list[DeliveryCallback] = []

    def produce(self, topic: str, *, key: str, value: bytes, on_delivery: DeliveryCallback) -> None:
        self.messages.append((topic, key, value))
        self._pending.append(on_delivery)

    def flush(self, timeout: float) -> int:
        for callback in self._pending:
            callback(None, None)
        self._pending.clear()
        return 0
