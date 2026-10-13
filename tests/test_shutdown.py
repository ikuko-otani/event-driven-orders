"""A termination signal becomes a stop request instead of ending the process."""

import signal

import pytest

from common.shutdown import stop_on_sigterm


@pytest.mark.asyncio
async def test_sigterm_sets_the_stop_event_instead_of_ending_the_process() -> None:
    previous = signal.getsignal(signal.SIGTERM)
    try:
        stop = stop_on_sigterm()

        signal.raise_signal(signal.SIGTERM)

        assert stop.is_set()
    finally:
        # The handler is process-wide: give the rest of the test run its own back
        signal.signal(signal.SIGTERM, previous)
