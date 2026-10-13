"""Turn a termination signal into a request to stop, for the resident loops.

A container is stopped with SIGTERM, then SIGKILL once its grace period runs
out. A loop that checks the returned event between units of work finishes the
one in hand and returns within that period, instead of being killed mid-way.
"""

import signal
import threading
from types import FrameType


def stop_on_sigterm() -> threading.Event:
    """Return an event that is set once the process is asked to stop."""
    stop = threading.Event()

    # Only record the request: a handler runs in between two steps of whatever
    # was executing, so the loop itself decides when it is safe to leave
    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    return stop
