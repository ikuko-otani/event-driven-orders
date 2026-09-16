# Technical Debt

Known gaps that are deliberately left unfixed, registered at the moment they are identified rather than when they are scheduled.
An entry is closed by a commit that removes the gap, not by deciding to remove it later.

| ID | Summary | Status |
|---|---|---|
| TD-001 | The retry path emits no log line, so a retrying consumer is unobservable | **Closed** |
| TD-002 | No engine validates a pooled connection before use | Open |
| TD-003 | A poller that exits is never restarted | Open |
| TD-004 | A trace stops at the broker, so one order's work is several traces | Open |

---

## TD-001: The retry path emits no log line

**Status**: Closed by the structured-logging work.
**Identified**: 2026-09-11, while driving the failure paths against the running stack.
**Closed**: 2026-09-16.

### What is missing

`handle_with_retry` writes nothing on an attempt that fails.
The repository has two log lines in total, and neither of them sits on the retry path: the first fires only once the message has already been copied to the dead-letter topic, and the second reports an unexpected state transition.
A consumer that is retrying therefore looks identical to a consumer that is idle.

### How it fails

A message that failed five times and was dead-lettered produced exactly one line of output, after 56 seconds of silence.
Nothing recorded which attempt was running, how long each wait was, or what the failure was before the last one.
An operator watching the logs during an incident sees a consumer that has stopped reporting, and cannot tell it apart from one that has hung.

### What closing it takes

The structured-logging work already carries the pipeline this needs.
The gap is registered separately because that work is itself a candidate for deferral, and the two must not be closed by assumption.
`handle_with_retry` now writes one `event_handle_failed` line per failed attempt, carrying the attempt number, the ceiling it is waiting under, and the error that caused it.
The silence this entry describes is gone: the 56 seconds that produced one line now produce five.

---

## TD-002: No engine validates a pooled connection before use

**Status**: Open.
**Identified**: 2026-09-11, after restarting the database underneath a running stack.

### What is missing

All six SQLAlchemy engines are built from a URL and nothing else:

```python
create_async_engine(DatabaseSettings().async_url)
create_engine(DatabaseSettings().sync_url)
```

None sets `pool_pre_ping`, so a connection is handed out without checking that the server is still on the other end of it.

### How it fails

Restarting the database leaves every pooled connection dead while the pool still believes it is usable.
The next request fails with `InterfaceError: connection is closed` and the HTTP service returns 500.
SQLAlchemy invalidates the whole pool once it recognises the disconnect, so a single request absorbs the damage and the next one succeeds — but that request belongs to a caller, and there is no retry in front of it.
The consumers recover on their own, because a failed attempt is a retry rather than a response; the HTTP service has no such layer.

### What closing it takes

`pool_pre_ping=True` on every engine.
Six call sites set the same options independently, which is the reason this went unnoticed in all six: a shared factory in `common/db.py` would make the setting a single decision rather than six identical ones, and is the better fix.

---

## TD-003: A poller that exits is never restarted

**Status**: Open.
**Identified**: 2026-09-11, after stopping the database underneath a running stack.

### What is missing

No service in `compose.yaml` declares a `restart` policy.

### How it fails

Stopping the database killed both poller processes, which exited with status 1 and stayed down.
Both consumers survived the same outage, because a consumer wraps its work in a retry loop and a poller does not.
Nothing is lost when a poller dies — its unpublished rows are still in the outbox, and it resumes from them when it starts — but nothing starts it, so publication stops until someone runs `docker compose up -d` by hand.
A single-command local start is an explicit goal of this stack, and a stack that needs a second command after any database blip does not meet it.

### What closing it takes

`restart: unless-stopped` on the application services.
This is the local answer only: a scheduler restarts a failed task in the deployment shape this project plans, so the entry covers the Compose stack rather than the deployed one.
Restarting a poller is safe under the singleton rule, since the restarted process is the same single instance, not a second one.

---

## TD-004: A trace stops at the broker

**Status**: Open.
**Identified**: 2026-09-16, while adding spans to the processing paths.

### What is missing

The event envelope has eight fields, and none of them carries trace context.
The outbox table has no column for it either.
A span opened in one process therefore has no way to name a span in another as its parent, and the four processes an order passes through produce four unrelated traces rather than one.

### How it fails

The question "where did this order spend its time" cannot be answered from a trace.
Each trace answers it for one process — the confirm, the publish, the reservation, the compensation — while the waits between them go unrecorded, and in a queue-based system those waits are where the time actually goes.
Correlating them by hand is possible through the log lines, since every line carries the order or event id, but that is a search rather than a picture.

### What closing it takes

A field on the envelope carrying W3C trace context, a column on both outbox tables to persist it, and one Alembic migration per service.
The consumer then continues the trace it is handed instead of starting one, and the poller passes the value through without reading it, which is the only role the design gives it.
This is deliberately deferred: the design names observability as a reduction candidate and settles on structured logging, so the connected trace is an addition to that decision rather than a gap in it.
