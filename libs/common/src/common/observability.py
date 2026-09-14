"""The one logging pipeline every process installs at start-up (design §2).

Correlating one order across five processes and a Kafka hop depends on
order_id, event_id and trace_id being fields rather than words in a sentence:
a formatted line cannot be filtered or aggregated by them. Every line
therefore leaves here as JSON — including the ones uvicorn and SQLAlchemy
write through the standard library, so there is no second stream to search.
"""

import logging

import structlog
from structlog.typing import Processor

# What every line carries, whichever library wrote it. Bound context comes
# first so a field bound once at start-up reaches lines written long after.
SHARED: list[Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
]


def configure(service: str) -> None:
    """Install the JSON pipeline for this process, before anything else logs.

    Every entrypoint calls this first: a line written beforehand is formatted
    by whatever default was still in place, and arrives as text in the middle
    of a JSON stream.
    """
    # One handler renders both sources: structlog's own events, and the records
    # libraries emit through the standard library. foreign_pre_chain is how a
    # library's record picks up the shared fields it never asked for.
    handler = logging.StreamHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=SHARED,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
        )
    )

    # The root logger owns that one handler: replacing the list rather than
    # adding to it keeps a second configure() call from doubling every line.
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)

    # structlog stops at wrap_for_formatter and hands its event dict to the
    # handler above instead of rendering it, which is what puts both sources
    # through one renderer.
    structlog.configure(
        processors=[*SHARED, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Bound once and merged into every later line: five processes write to one
    # stream in compose, so a line that cannot say which one wrote it is close
    # to unusable.
    structlog.contextvars.bind_contextvars(service=service)
