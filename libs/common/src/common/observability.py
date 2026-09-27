"""The one logging pipeline every process installs at start-up (design §2).

Correlating one order across five processes and a Kafka hop depends on
order_id, event_id and trace_id being fields rather than words in a sentence:
a formatted line cannot be filtered or aggregated by them. Every line
therefore leaves here as JSON — including the ones uvicorn and SQLAlchemy
write through the standard library, so there is no second stream to search.
"""

import logging

import structlog
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from structlog.typing import EventDict, Processor, WrappedLogger

from common.settings import TracingSettings


def _stamp_trace(_logger: WrappedLogger, _method: str, event: EventDict) -> EventDict:
    """Put the span a line was written inside onto the line itself.

    Read at the moment of the call, never bound ahead of it: the current span
    changes many times within one process, and a value bound for one of them
    would still be sitting there for the next. A context that is not valid
    means the line was written outside any span, and it carries no trace
    fields at all rather than empty ones.
    """
    context = trace.get_current_span().get_span_context()
    if context.is_valid:
        event["trace_id"] = trace.format_trace_id(context.trace_id)
        event["span_id"] = trace.format_span_id(context.span_id)
    return event


# What every line carries, whichever library wrote it. merge_contextvars runs
# first so that anything bound for the work in hand is already in the dict the
# processors after it read.
SHARED: list[Processor] = [
    structlog.contextvars.merge_contextvars,
    _stamp_trace,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
]


def _stamp_service(service: str) -> Processor:
    """Build the processor that names this process on every line.

    A processor, not a bound context variable: five processes write to one
    stream in compose, and the name has to reach lines written from any task,
    including the ones uvicorn logs outside the context the pipeline was
    installed in.
    """

    def stamp(_logger: WrappedLogger, _method: str, event: EventDict) -> EventDict:
        event["service"] = service
        return event

    return stamp


def _configure_tracing(service: str) -> None:
    """Give this process a tracer, and its spans somewhere to go (design §2).

    The provider is installed whether or not there is a collector: a span
    still gets a trace id that way, which is what the log lines carry. Only
    the export is conditional.
    """
    # service.name is what Jaeger groups traces by, so it is given the same
    # name the log lines are stamped with — one word finds a process in both.
    provider = TracerProvider(resource=Resource.create({"service.name": service}))

    # Batched, so no span makes the work that created it wait for an HTTP round
    # trip. Attached only when an endpoint exists, since an absent collector
    # would otherwise be retried on every batch.
    endpoint = TracingSettings().exporter_otlp_endpoint
    if endpoint:
        # /v1/traces is appended here: the SDK appends it only when it reads
        # the variable itself, and takes a constructor argument verbatim.
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
        )

    trace.set_tracer_provider(provider)


def configure(service: str) -> None:
    """Install this process's logging and tracing, before anything else logs.

    Every entrypoint calls this first: a line written beforehand is formatted
    by whatever default was still in place, and arrives as text in the middle
    of a JSON stream.
    """

    # The process name is fixed for this process, so it joins the shared list
    # once here rather than being bound and merged per line.
    shared: list[Processor] = [_stamp_service(service), *SHARED]

    # One handler renders both sources: structlog's own events, and the records
    # libraries emit through the standard library. foreign_pre_chain is how a
    # library's record picks up the shared fields it never asked for.
    handler = logging.StreamHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared,
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
    # through one renderer. Loggers are not cached: a cached logger keeps the
    # processors it first saw, so a second configure() would never reach it.
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    # Both halves are installed together: a trace id is only useful because the
    # log lines carry it, so no process wants one without the other.
    _configure_tracing(service)


def adopt_loggers(*names: str) -> None:
    """Hand a library's own loggers to the root, so its lines join the pipeline.

    A library that installs handlers of its own and turns off propagation —
    uvicorn does both — keeps writing in its own format from its own stream,
    which is the second pipeline §6.6 exists to prevent. Emptying its handlers
    leaves it with nothing to write to but the root logger configured above.
    """
    # Both halves are needed: a handler left in place would print the line a
    # second time, and propagation left off would stop it reaching the root.
    for name in names:
        adopted = logging.getLogger(name)
        adopted.handlers = []
        adopted.propagate = True
