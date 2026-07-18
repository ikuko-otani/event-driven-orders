# ADR-001: Kafka as the Event Broker

## Status

Accepted — 2026-07-18

## Context

The Saga design (domain-design §5) integrates order-api and inventory-worker exclusively through asynchronous events;
the broker is the **only** integration path between the two services (§3.1).

The domain design already commits to properties the broker must supply:

- **Per-order ordering with parallel consumers** — all events of one order share a message key (`order_id`) and must be consumed in publish order (§4.2), while the consumer side scales horizontally (§6.2–6.3).
- **At-least-once delivery under consumer control** — the consumer must be able to commit its position only *after* its DB transaction commits (§5.7); duplicates are absorbed by `processed_events` (§4.4).
- **Replayability** — consumption must be non-destructive and position-addressable: DLQ re-injection (§5.6) re-publishes a delivered message's original bytes to its topic, and event-replay integration tests (§1) re-run the real consumers over recorded event sequences. Both need a log that keeps delivered messages and lets a consumer re-read from a chosen position, not a queue that drops them on ack.
- **Retention as configuration** — DLQ topics must hold messages for 14 days regardless of consumption (§5.6).
- **Producer idempotence** — the outbox poller relies on `enable.idempotence=true` to suppress duplicate sends within a producer session (§5.4).

Three candidates were considered:

1. **Apache Kafka** (via a Kafka-API-compatible broker)
2. **Redis Streams** — attractive because Redis is already in the stack for HTTP idempotency keys (§4.4)
3. **RabbitMQ** — the most common general-purpose message broker

## Decision

Use **Kafka** (the Kafka protocol and client API) as the event broker.

- **Local development and CI**: a **single-node Redpanda** container in Compose, and the same image via testcontainers in CI.
- **Production (assumption, not deployed in v0.5)**: a managed Kafka service — Amazon MSK or Confluent Cloud (see below).
  v0.5 deliberately operates no broker in the cloud; the AWS deployment scope is ADR-003.

## Rationale

| Requirement (Context) | Kafka | Redis Streams | RabbitMQ |
|---|---|---|---|
| Keyed ordering + parallel consumers | Built-in (key → partition) | Manual sharding | Plugin + manual discipline |
| Consumer-controlled at-least-once | Manual offset commit | PEL + manual claiming | Manual ack |
| Replay of delivered messages | Offset rewind | Works against group bookkeeping | Deleted on ack |
| Time-based retention | Per-topic config | Size/ID-based trimming only | N/A (queue empties) |
| Idempotent producer | Built-in | Not available | Not available |
| Ecosystem / operational knowledge | De facto standard | Niche for this role | Mature, but for queueing |

### Why not Redis Streams — despite Redis already being in the stack

The strongest argument for Redis Streams is economic: Redis already runs in this system, so the broker would be "free".
The costs surface at exactly the properties the design depends on:

- **Ordering vs parallelism is either/or.**
  A stream has no partition concept: its entries are totally ordered, but a consumer group delivers entries to competing consumers, so per-key ordering is lost as soon as there is a second consumer.
  Recovering both at once means sharding into N streams and implementing `order_id` → stream routing by hand — re-implementing Kafka's partitioning at the application layer, where it is code to maintain instead of broker configuration.
- **Replay works against the grain.**
  Historical entries can be re-read with ID-range queries (`XRANGE`), but consumer-group state (last-delivered ID, pending-entries list) is built for forward consumption and manual claiming (`XAUTOCLAIM`), not for rewind-and-reprocess.
  DLQ re-injection (§5.6) and event-replay tests (§1) are rewind workflows.
- **Role mismatch on durability.**
  The Redis already in the stack fronts HTTP idempotency as a response cache with a 24-hour TTL — a cache-tier responsibility where losing data on restart is acceptable precisely because the durable authority is the `orders.idempotency_key` constraint in Postgres, not Redis (§4.4).
  A 14-day event log is a system-of-record responsibility; colocating it on the same in-memory-first instance couples the log's memory footprint and availability to the cache, and running a second, persistence-hardened Redis forfeits the "already have it" economy that motivated the option.
- **Trimming is not retention.**
  Streams are capped by length (`MAXLEN`/`MINID`); a 14-day time-based DLQ retention (§5.6) would need an external trimming job.

The "free" broker saves one container in Compose and pays for it with application-level partitioning, replay machinery, and retention jobs.

### Why not RabbitMQ

- **The smart-broker model consumes destructively.**
  RabbitMQ routes messages to queues and deletes them on acknowledgement; a consumed message is gone.
  Offset rewind, event-replay tests, and re-reading a DLQ'd message's context all disappear with it.
- **Same ordering dilemma as Streams.**
  A single queue preserves order only with a single consumer; competing consumers interleave.
  Per-key ordering requires the consistent-hash exchange plugin plus a one-consumer-per-queue discipline — again, partitioning rebuilt by hand.
- **RabbitMQ Streams is not a rescue.**
  The Streams feature (3.9+) adopts a Kafka-like log model, but choosing it forfeits RabbitMQ's actual strength (mature AMQP routing) while buying a younger log implementation with a fraction of Kafka's ecosystem — the comparison collapses back to "Kafka, but less of it".
- **Right tool, wrong job.**
  RabbitMQ excels at task queues, RPC, and complex routing topologies.
  This system needs an ordered, replayable event log; none of those strengths apply to it.

### Why Kafka

Kafka's log-based model supplies every Context property as broker-native configuration:
consumption does not delete (replay and retention are free), key → partition mapping gives per-order ordering *with* consumer-group parallelism, offsets are consumer-controlled, and the idempotent producer is built in.

The reliability analysis in the domain design is already written against these primitives —
`enable.idempotence` (§5.4), manual offset commit ordering (§5.7), cooperative-sticky rebalancing and `max.poll.interval.ms` (§5.5, §6.3), partition-count arithmetic (§6.2).

Beyond the mechanics, Kafka is the **de facto standard** for event streaming:
it has the richest client/tooling/documentation base, production experience is widely documented in public engineering write-ups, and multiple managed offerings provide credible production paths.
For an event-driven system, that ecosystem depth is itself an architectural property — operational answers exist before the questions are asked.

> **Scope note on the ordering argument.**
> The per-order ordering claim inherits the documented precondition of §5.7: at most one event per order is in flight at any time.
> If that ever changes (e.g. `OrderCancelled`, §4.5), the ordering analysis must be redone — choosing Kafka does not exempt this ADR from that caveat.

## Local development: single-node Redpanda

- **Kafka-API-compatible single binary** — no ZooKeeper ensemble, and no separate KRaft controller quorum to configure; one container in Compose.
- **Light footprint** — written in C++ with modest memory defaults, it suits a laptop and CI runners; a testcontainers module exists for the CI path (§2).
- **Single node = replication factor 1**, so `acks=all` degenerates to a single broker's acknowledgement.
  Accepted: local data durability is not a requirement, and the producer contract of §5.4 is unchanged — on a replicated cluster the same configuration provides full guarantees **once `min.insync.replicas ≥ 2`**, so `acks=all` waits for a real replica quorum rather than a lone in-sync leader.
- **Known limitation**: Redpanda is protocol-compatible, not Kafka's codebase.
  v0.5 relies only on core protocol surface (idempotent producer, consumer groups, manual commit) — all supported; broker-internal behaviour (e.g. exact rebalance timing) may differ from Kafka's, which is acceptable for a dev/CI environment whose production assumption is stated below.

## Production assumption (not deployed in v0.5)

v0.5 runs the broker in local Compose only; the planned AWS deployment (ADR-003) covers order-api + RDS, not a broker.
If this system went to production, the broker would be **managed Kafka**, with two credible paths:

- **Amazon MSK** — stays inside the AWS account/VPC of ADR-003 (IAM auth, CloudWatch, private networking); broker and partition sizing remain the operator's job.
- **Confluent Cloud** — higher abstraction (serverless tier) and a bundled schema registry, which is the designated home for `event_version` payload evolution (§4.1).

**Self-managed Kafka on EC2 is rejected outright**: broker operations (storage scaling, partition rebalancing, version upgrades, failure recovery) are a continuous operational burden that managed offerings absorb entirely, and at this system's scale there is no cost or control requirement that would justify carrying it.

The MSK-vs-Confluent choice is deliberately left as an assumption, not a decision:
nothing in v0.5 depends on it, both are protocol-compatible with the local environment, and deciding without a real deployment's constraints (budget, VPC topology, team) would be speculation.

## Consequences

- `compose.yaml` carries one `redpanda` service; CI uses the same image via testcontainers — dev/CI parity by construction.
- The reliability design (§5.4–§5.7) is written against Kafka primitives; moving to a non-Kafka broker later would mean revisiting that analysis, not just swapping a client library.
  This lock-in is accepted and now documented.
- No schema registry exists in v0.5; `event_version` in the envelope (§4.1) is the placeholder, and enforcement is deferred to the production path or a stretch task.
- The Python client must support the idempotent producer, manual offset commit, and cooperative-sticky assignment (e.g. `confluent-kafka-python`).

## References

- `docs/design/domain-design.md` §1, §3.1, §4.2, §4.4, §5.4–§5.7, §6
- [Kafka documentation — design](https://kafka.apache.org/documentation/#design)
- [Redis Streams introduction](https://redis.io/docs/latest/develop/data-types/streams/)
- [RabbitMQ Streams overview](https://www.rabbitmq.com/docs/streams)
- [Redpanda — Kafka compatibility](https://docs.redpanda.com/current/reference/kafka-compatibility/)
- [Amazon MSK](https://aws.amazon.com/msk/) / [Confluent Cloud](https://www.confluent.io/confluent-cloud/)
