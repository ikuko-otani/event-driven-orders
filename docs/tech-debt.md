# Technical Debt

Known gaps that are deliberately left unfixed, registered at the moment they are identified rather than when they are scheduled.
An entry is closed by a commit that removes the gap, not by deciding to remove it later.

| ID | Summary | Status |
|---|---|---|
| TD-001 | The retry path emits no log line, so a retrying consumer is unobservable | **Closed** |
| TD-002 | No engine validates a pooled connection before use | Open |
| TD-003 | A poller that exits is never restarted | **Closed** |
| TD-004 | A trace stops at the broker, so one order's work is several traces | Open |
| TD-005 | A write endpoint answers before its transaction commits, and caches the answer first | **Closed** |
| TD-006 | The poller treats every failed send as permanent, so a broker outage quarantines healthy rows | **Closed** |
| TD-007 | `docker compose up` on a fresh volume starts the pollers before any schema exists | **Closed** |
| TD-008 | An oversized outbox row raises inside `produce()` and stops the poller | Open |
| TD-009 | A message that is JSON but not an envelope is retried as a technical failure | **Closed** |
| TD-010 | No test runs two confirms, or two reservations of one item, concurrently | **Closed** |
| TD-011 | The inventory consumer reserves stock for any event type on its topic | **Closed** |
| TD-012 | Tests that assert on log lines depend on a fixture in another file having run first | **Closed** |
| TD-013 | The dead-letter and quarantine log lines do not say which message or row failed | **Closed** |
| TD-014 | Two things the design describes do not exist: `DELETE /orders/{id}` and the re-injection script | Open |
| TD-015 | No process handles SIGTERM, so a stop waits ten seconds and ends in SIGKILL | Open |
| TD-016 | No test drives one order through both relays and both consumers | **Closed** |

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
This is deliberately deferred to the deployment work: a managed database's maintenance restarts make this a routine event there rather than a hand-made one, so the fix is verified against the failure it exists for.

---

## TD-003: A poller that exits is never restarted

**Status**: Closed.
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
A span opened in one process therefore has no way to name a span in another as its parent, and the five processes an order passes through produce five unrelated traces rather than one.

### How it fails

The question "where did this order spend its time" cannot be answered from a trace.
Each trace answers it for one process — the confirm, the publish of its event, the reservation, the publish of the reply, and the applying of the outcome — while the waits between them go unrecorded, and in a queue-based system those waits are where the time actually goes.
Correlating them by hand is possible through the log lines, since every line carries the order or event id, but that is a search rather than a picture.

### What closing it takes

A field on the envelope carrying W3C trace context, a column on both outbox tables to persist it, and one Alembic migration per service.
The consumer then continues the trace it is handed instead of starting one, and the poller passes the value through without reading it, which is the only role the design gives it.
This is deliberately deferred: the design names observability as a reduction candidate and settles on structured logging, so the connected trace is an addition to that decision rather than a gap in it.

---

## TD-005: A write endpoint answers before its transaction commits, and caches the answer first

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-17.

### What is missing

`get_session` commits after the request handler returns, and FastAPI runs that exit code after the response has been sent.
`POST /orders` also writes its response into the Redis cache before the commit runs.
Nothing in the request path commits before the client is answered.

### How it fails

With a deferred constraint made to fail at commit, a client received `201 Created`, the database held no order, and the cache held the phantom response for 24 hours.
A retry with the same idempotency key was answered `200` from the cache with an order id that `GET /orders/{id}` reports as `404`.
The confirm endpoint has the same shape: `200` leaves before the `PENDING → CONFIRMED` transition and its outbox row are durable, which is the opposite of what design §4.6 argues when it chooses `200` over `202`.

### What closing it takes

An explicit `await session.commit()` in each write route before it returns, with the cache write moved after it.
`get_session` then guarantees rollback only.
One test that fails the commit and asserts a 5xx, an empty cache, and a successful retry.
Each write route now commits before it returns, and the response cache is written only after that commit.
The session dependency guarantees the rollback and nothing else, since its cleanup runs after the response has already been sent.
A test refuses the commit with a deferred constraint trigger and asserts the 500, the empty cache, and a retry that still creates the order.

---

## TD-006: The poller treats every failed send as permanent, so a broker outage quarantines healthy rows

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-17.

### What is missing

`publish_batch` classifies a failed send with `error.retriable()`.
The error object the real client hands to a delivery callback never sets that flag, so it is `False` for every failure, a timed-out send included.

### How it fails

With the broker stopped for longer than `message.timeout.ms`, every row in the batch was quarantined on its first failure and logged as permanent.
Once the broker was back, the rows stayed quarantined and their orders stayed `CONFIRMED`.
The retry-on-the-next-cycle path of design §5.4 never runs, and the six unit tests that cover it pass only because the fake producer can report a retriable error that the real one cannot.

### What closing it takes

Classify by error code rather than by the flag: a short list of permanent codes, everything else transient and bounded by `max_attempts`.
One test against a real producer pointed at an unreachable address, asserting that the row is not quarantined after one failure.
The decision belongs in `common/kafka.py`, the one module that knows the client's constants.
The classification is now a list of permanent error codes held in `common/kafka.py`, the one module that may know the client's constants, and the loop is handed the plain integers.
Anything not on that list is transient and bounded by `max_attempts`, so an unlisted code costs a few attempts rather than a permanent quarantine.
The fake delivery error carries a code instead of a flag it could choose, so a unit test can no longer describe a failure the real client cannot report.
A test publishes to an address nothing listens on and asserts the row keeps its place in the outbox after the send times out.

---

## TD-007: `docker compose up` on a fresh volume starts the pollers before any schema exists

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.

### What is missing

`compose.yaml` has no service that applies the two Alembic histories.
The application processes wait only for the database to be healthy, and the migration and seed tasks run on the host by hand.

### How it fails

On a fresh clone, `docker compose up -d` leaves both pollers in `Exited (1)` on `UndefinedTable`, while the HTTP service reports healthy and answers `500` to any write.
The single-command start this stack promises does not hold for the first start, which is the one a new reader performs.

### What closing it takes

Two one-shot `migrate` services rather than one: each image is synced with `--package`, so the order-api image does not contain the inventory package that the inventory history's `env.py` imports.
Each application process waits with `service_completed_successfully` on the one-shot for its own schema, which keeps the service boundary of design §3.1 intact in Compose as well.
A restart policy on the application services, so a process that loses a race with its dependencies is started again rather than left down.

---

## TD-008: An oversized outbox row raises inside `produce()` and stops the poller

**Status**: Open.
**Identified**: 2026-09-16, during an adversarial review of the implementation.

### What is missing

The client rejects a message over its size limit synchronously, from `produce()`, not through the delivery callback.
`publish_batch` catches nothing around `produce()`, and neither does the loop above it.

### How it fails

A 2 MB value raised `KafkaException(MSG_SIZE_TOO_LARGE)` before any callback ran, which ends the poller process.
On restart the same row is selected first and the process ends again.
Since the restart policy landed, that restart is automatic and unattended, so the poller now crash-loops on the row rather than staying down — a relay that is running and publishing nothing is harder to notice than one that has visibly stopped.
The line cap on `POST /orders` keeps a legitimate order far below the limit, so today this needs a hand-written row; the containment the design promises in §5.7 is nevertheless absent.

### What closing it takes

A `try` around `produce()` that records the row as a permanent failure, sharing the classification of TD-006.
A fake producer entry that raises on `produce()`, and one test.
That classification cannot simply be reused, however: `permanent_errors` is matched against a delivery report's `code()`, while `produce()` raises an exception whose type this loop may not name, because it does not import the client.
Closing this therefore settles an interface — a classifier handed in through `PollerConfig`, or a bare `except Exception` bounded by `max_attempts` — rather than only adding a `try`.
This is deliberately deferred: nothing the services write comes near the size limit, so settling the interface now would fit it to a failure that only a hand-written row produces.

---

## TD-009: A message that is JSON but not an envelope is retried as a technical failure

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-27.

### What is missing

Only a failure of `json.loads` is treated as permanent.
A document that parses but lacks `event_type` or `event_id`, or whose `event_id` is not a UUID, or that is a list or a string, raises a plain `KeyError`, `ValueError` or `TypeError`.

### How it fails

Each of those shapes ran the full schedule: four waits and five attempts before the dead-letter copy, with `KeyError` as the recorded error class.
The partition is blocked for up to fifteen seconds to reach an outcome that was certain on the first attempt.

### What closing it takes

A shape check straight after parsing that raises `PermanentFailure`, and one parametrized test case per shape alongside the existing not-JSON test.
`_parse_envelope` now judges the shape straight after decoding: a document that is not an object, has no string `event_type`, or has no `event_id` that parses as a UUID raises `PermanentFailure` and is dead-lettered on its first attempt.
The dead-letter path calls the same function, so what counts as an envelope is decided in one place.
A parametrized test sends five such shapes and asserts that none of them waits out the retry schedule.

---

## TD-010: No test runs two confirms, or two reservations of one item, concurrently

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-17.

### What is missing

The confirm tests call the endpoint twice in sequence, which a read-then-write implementation would also pass.
The reservation test observes a held lock with `NOWAIT` rather than letting two orders compete for the same unit.

### How it fails

The conditional `UPDATE` that closes the double-confirm path, and the row lock that serialises reservations, are the two mechanisms the design leans on hardest, and the suite would stay green if either were replaced by a read-then-write.
Both were exercised by hand during the review and held; the suite does not say so.

### What closing it takes

Ten concurrent confirms of one `PENDING` order through the ASGI client, asserting one outbox row.
Two reservations of the last unit of one item on two threads, asserting one `Reserved` and one `Insufficient`.
Ten concurrent confirms of one PENDING order now run through the ASGI client and assert a single outbox row.
Two threads, released together by a barrier, reserve the last unit of one item and assert one Reserved, one Insufficient, and one reservation row.

---

## TD-011: The inventory consumer reserves stock for any event type on its topic

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-20.

### What is missing

`handle_order_confirmed` never reads `event_type`.
The order-api consumer dispatches on it and ignores what it has no transition for; the inventory consumer has no such guard.

### How it fails

An envelope with `event_type` set to `OrderCancelled` and an `OrderConfirmed` payload reserved the order's stock.
Nothing produces such an event today, but the design reserves that name for the same topic, and the first event added there would be reserved as if it were a confirmation.

### What closing it takes

One early return on the event type, and the test above.
`handle_order_confirmed` now returns on any envelope whose `event_type` is not `OrderConfirmed`.
A test sends an envelope named `OrderCancelled` carrying a confirmation's payload, and asserts that no reservation row and no outbox row follow.
The two consumers now have the same shape: each names the events it has work for, and leaves the rest of its topic to whoever does.

---

## TD-012: Tests that assert on log lines depend on a fixture in another file having run first

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-27.

### What is missing

Nothing in the test session configures the logging pipeline.
It is configured as a side effect of the HTTP client fixture's lifespan, in tests that happen to sort earlier.

### How it fails

`pytest tests/test_order_reservation_events.py -k unknown_order` fails on its own: the warning goes to the unconfigured default logger, and `caplog` captures nothing.
The sibling assertion that no warning was logged passes for the same reason, without checking anything.

### What closing it takes

A session-scoped, autouse fixture that installs the pipeline once, or `structlog.testing.capture_logs()` in place of `caplog`, as the observability tests already do.
Replacing `caplog` was not enough on its own.
The pipeline cached each logger on first use, and every test that starts the HTTP client calls `configure()` again, so a logger first used before that call kept processors that `capture_logs()` never reaches; under the full suite, the redelivery test captured nothing.
The pipeline no longer caches loggers, which is what `configure()` already assumed when it replaced the root handler rather than adding one: that a second call reaches every logger.
Both tests now use `capture_logs()`, and the one asserting that no warning was logged also asserts the two `event_handled` lines it did capture, so an empty capture fails instead of passing.

---

## TD-013: The dead-letter and quarantine log lines do not say which message or row failed

**Status**: Closed.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-27.

### What is missing

`event_dead_lettered` carries the topic it went to, the consumer, the attempt count and the error class, but not the source topic, partition and offset, nor the event id.
`outbox_publish_failed` carries two counts and no row id or error code.

### How it fails

An operator reading the log knows that something was dead-lettered or quarantined and nothing about what.
The recovery step of design §5.7 asks them to inspect "the error the poller logged for it", and no such line exists.

### What closing it takes

The message position and, where the envelope parsed, its id on the dead-letter line.
One line per quarantined row, with its id and the broker's error name, on the poller.
`event_dead_lettered` now carries the source topic, partition and offset, the event id when the envelope parsed and `null` when it did not, and the error message, which a permanent failure otherwise left only in the dead-letter headers.
The poller writes `outbox_row_quarantined` once per quarantined row, with the row id, the aggregate id, the attempt count, and the broker's error code and name; the UPDATE returns the rows it touched, because the attempt limit is judged inside the statement.
`outbox_publish_failed` stays one count per cycle, since it is the line that repeats for as long as the broker is down.
Tests assert the fields of each line, and that a row still being retried produces no quarantine line.

---

## TD-014: Two things the design describes do not exist: `DELETE /orders/{id}` and the re-injection script

**Status**: Open.
**Identified**: 2026-09-16, during an adversarial review of the implementation.

### What is missing

Design §4.6 catalogues `DELETE` and `PUT` on an order; the router has neither.
Design §5.6 describes a script that reads the dead-letter topic and republishes selected messages to their original topic; `scripts/` holds the seed script only.

### How it fails

A reader following the state × operation table gets `405` for a documented cell.
A dead-lettered order can only be recovered with hand-typed broker commands, and two such messages have been waiting since the failure paths were first exercised.

### What closing it takes

`DELETE` in the shape of `PATCH`: a locking read, `409` outside `PENDING`, `204` on success.
A small script that consumes the dead-letter topic and republishes by original key and topic, selectable by event id.
If the script is not built, design §5.6 should say what the manual procedure is instead.
This is deliberately deferred: neither lies on the path an order takes from intake to reservation, and what the script would automate can be done today with the broker's own commands.

---

## TD-015: No process handles SIGTERM, so a stop waits ten seconds and ends in SIGKILL

**Status**: Open.
**Identified**: 2026-09-11, while stopping the stack after exercising the failure paths; registered 2026-09-16.

### What is missing

Both `run_forever` loops run until an exception ends them.
Nothing turns a termination signal into a request to finish the current unit of work and leave.

### How it fails

`docker compose stop` waits its grace period on every application container and then kills it; the exit codes on record are `143` and `137`.
Correctness survives, because publish-before-mark and claim-before-commit already assume abrupt death.
What is lost is the spans still queued in the batch exporter, the consumer's group departure (its partitions stay assigned until the session times out), and ten seconds on every stop.

### What closing it takes

A signal handler that sets a flag, a loop condition that reads it, and `close()` on the consumer on the way out.
This is deliberately deferred to the deployment work: a rolling deployment is what sends SIGTERM as a matter of routine, and the grace period the handler must fit within is set there.

---

## TD-016: No test drives one order through both relays and both consumers

**Status**: Closed by the Compose smoke check.
**Identified**: 2026-09-16, during an adversarial review of the implementation.
**Closed**: 2026-09-19.

### What is missing

The broker-backed tests cover one hop each: outbox to topic, and topic to handler.
No test confirms an order and asserts that it reaches `RESERVED` through the real poller, the real consumer loop, the second outbox and the second consumer.

### How it fails

The end-to-end path was verified by hand against a running stack during the review and worked in under a second.
The suite that gates every merge does not exercise it, although the design names event-replay integration tests as the mitigation for the relay being first-party code.

### What closing it takes

A check that drives the path rather than one that reassembles it.
`scripts/smoke.py` confirms an order against a running stack and polls `GET /orders/{id}` until the status is `RESERVED`, which it can reach only through both relays and both consumers, running as the processes Compose starts rather than as calls a test makes in order.
CI runs it in a job of its own, so every pull request exercises the path; the same check runs locally with `uv run poe smoke`.
It asserts the outcome and not the stages in between: a test that needs to observe one hop still drives that stage directly, as the broker-backed tests do.
