# ADR-002: Custom Outbox Poller over Debezium CDC

## Status

Accepted — 2026-07-18

## Context

Both services publish events through a transactional outbox (domain-design §3.2):
the event is written to the `outbox` table in the same DB transaction as the business change, and a separate relay moves committed rows to Kafka.
The outbox *table* is settled design; this ADR decides the **relay**.

Two standard implementations exist:

1. **Polling publisher** — a process periodically queries the outbox table for unpublished rows, publishes them, and marks them.
2. **Transaction-log tailing (CDC)** — a change-data-capture tool reads the database's write-ahead log and publishes changes to Kafka.
   The standard tooling is **Debezium**, run on Kafka Connect, with its outbox event router SMT purpose-built for this pattern.

Requirements on the relay, from the domain design:

- At-least-once delivery into Kafka, preserving per-aggregate send order (§4.2, §5.4).
- Latency compatible with the end-to-end budget: the confirm→reserved path crosses **two** relays inside a 1 s p99 target (§6.1).
- Throughput ceiling above the 500 events/s assumption (§6.3).

## Decision

Implement the relay as a **custom Python polling publisher**, one singleton process per service.

The full design — loop structure, publish-before-mark ordering, batch size, producer configuration, crash-window analysis — is specified in domain-design **§5.4** and **§5.7** and is not repeated here.

**Debezium CDC is recorded as a stretch item** (domain-design §7), not adopted in v0.5.

## Rationale

### What Debezium would buy

Honesty first — the CDC approach is genuinely superior on three axes:

- **Latency**: it reacts to the WAL in near real time instead of on a polling interval.
- **DB load**: it reads the replication stream instead of issuing repeated queries, and needs no `published_at` bookkeeping write per event.
- **Scale-out story**: one Connect cluster relays for any number of services; per-service pollers multiply instead.

None of these advantages is binding at this system's scale:

- The polling interval contributes ≤ 200 ms worst-case to a 1 s end-to-end budget, and the idle-cycle query is a near-empty partial-index scan (§5.4).
- The singleton poller's ceiling (~2,000–5,000 events/s in drain mode) clears the 500 events/s assumption several times over (§6.3).
- There are exactly two services (§3.1).

### What Debezium would cost

Adopting Debezium adds **three operational subsystems** to a two-service system:

- **A Kafka Connect runtime** — a JVM cluster with its own configuration, deployment, monitoring, and failure modes (task states, rebalances, the Connect REST API).
- **Connector lifecycle** — Debezium connector configuration, the outbox event router SMT, snapshot behaviour on first start and after slot loss.
- **PostgreSQL logical replication** — `wal_level=logical`, replication-slot management, and slot monitoring: an abandoned slot silently retains WAL until the disk fills, a failure mode unrelated to any business logic.

Against that, the entire polling relay is a page of pseudocode (§5.4) whose every crash window is enumerated and absorbed by existing mechanisms (§5.7).

### What Debezium would *not* buy

CDC does not change the delivery contract: Debezium is also at-least-once, so consumers still need `processed_events` deduplication (§4.4), and per-aggregate ordering still rests on the outbox write order and keyed partitioning (§4.2).
The reliability machinery on both sides of the relay stays identical — the choice swaps only the middle.

### The deciding argument

For this project, the relay is not plumbing to be outsourced — it is where the at-least-once guarantee is actually **created**:
publish-before-mark converts every crash window into a duplicate instead of a loss (§5.4), and that argument is the centrepiece of the reliability analysis (§5.7).
Demonstrating that machinery first-hand is an explicit goal of this repository (domain-design §1);
with Debezium, the same guarantees would exist as configuration inside a third-party runtime, and the failure-window analysis would be about someone else's code.

A team running many services in production would weigh this differently — the fixed cost of Connect amortizes across services, and relay code is not their deliverable.
That is the scenario the stretch item exists for.

### Revisit conditions

Replace the poller with Debezium when any of these becomes true:

- W16 load tests show the polling interval or the singleton ceiling actually binding (§6.5), or
- the number of services grows enough that per-service pollers outweigh one Connect cluster, or
- outbox `published_at` write amplification measurably pressures the DB.

## Consequences

- Compose runs **two additional processes** (one poller per service) under restart-on-crash supervision; their availability requirement is soft — a dead poller delays events but loses none (§5.4).
- The relay is first-party code: its bugs are ours.
  Mitigation is the enumerated crash-window analysis (§5.7) plus event-replay integration tests exercising the real path (§1).
- Every event costs an extra `UPDATE` (the `published_at` mark) — accepted write amplification; cleanup/retention of published rows is deliberately deferred (§5.4).
- **The migration path stays open by construction**: the outbox table schema (`id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload`) is the same contract Debezium's outbox event router consumes.
  Switching the relay later touches neither the producers' transactions nor the consumers' deduplication — it swaps only the relay process.

## References

- `docs/design/domain-design.md` §3.2, §4.2, §4.4, §5.4, §5.7, §6.1, §6.3, §6.5, §7
- ADR-001 (broker selection; producer configuration constraints)
- [microservices.io — Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) (with Polling publisher / Transaction log tailing sub-patterns)
- [Debezium — Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [PostgreSQL — Logical replication slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)
