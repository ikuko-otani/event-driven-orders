# Domain Design — event-driven-orders

> **Version**: 0.1 (draft) — 2026-07-13
> **Status**: work in progress. This document defines the domain model and
> event catalog before implementation starts. Broker selection rationale is
> documented separately in an ADR (planned).

---

## 1. Business Context

<!-- TODO: Summarize the order-management domain this system models:
     order intake → inventory reservation → shipment → billing.
     Grounded in real-world business flows observed in production
     order-management work; this project itself is a personal project. -->

## 2. Scope (v0.5)

<!-- TODO: In-scope capabilities for v0.5 and explicit non-goals.
     Stretch items (Debezium CDC, CQRS read model) are out of scope. -->

## 3. ER Diagram

<!-- TODO: Mermaid erDiagram covering Order / OrderLine / Item / Inventory
     and messaging-infrastructure tables (outbox, processed_events). -->

```mermaid
erDiagram
```

### 3.1 Entity notes

<!-- TODO: One subsection per entity: purpose, key columns, invariants. -->

## 4. Event Catalog

<!-- TODO: One entry per event:
     - Event name
     - Producer / consumer
     - Payload summary
     - Trigger condition (when it is emitted)
     - Idempotency key design -->

## 5. Saga Overview — inventory reservation failure

<!-- TODO: High-level compensation flow only. Detailed Saga / retry / DLQ
     design is planned for a later design iteration (W3). -->

## 6. Out of Scope / Stretch

<!-- TODO: Deferred items and why they are deferred. -->
