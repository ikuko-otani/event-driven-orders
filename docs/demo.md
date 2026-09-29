# Demo: one order reserved, one refused

This walkthrough drives the running stack through both outcomes of the reservation Saga, using only `curl` and the logs.
Every output below is copied from a real run; only the shell prompts are left out.

## Setup

Start the stack and seed it as in the [quick start](../README.md#quick-start), then read the ids the seed wrote.
There are no master-data endpoints, so they come from the database directly.

```bash
q() { docker compose exec -T postgres psql -U app -d event_driven_orders -tAc "$1"; }
ENTITY_ID=$(q "select id from orders.sales_entities where code = 'ENT-01'")
CUSTOMER_ID=$(q "select id from orders.customers where code = 'CUST-01'")
ITEM_ID=$(q "select id from orders.items where code = 'ITEM-01'")
```

The seed stocks 100 units of each item.
The two scenarios differ only in the quantity ordered and the idempotency key.

## Scenario 1: stock is available

Create an order for 2 units, then confirm it.

```bash
ORDER_ID=$(curl -s -X POST localhost:8000/orders \
  -H "X-Entity-Id: $ENTITY_ID" -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
  -d @- <<EOF | jq -r .id
{"customer_id": "$CUSTOMER_ID", "currency": "JPY", "delivery_date": "2026-10-15",
 "lines": [{"item_id": "$ITEM_ID", "quantity": 2}]}
EOF
)
curl -s -X POST localhost:8000/orders/$ORDER_ID/confirm -H "X-Entity-Id: $ENTITY_ID" | jq '{order_number, status}'
```

```json
{
  "order_number": "ORD-000004",
  "status": "CONFIRMED"
}
```

The confirmation is answered before any stock is touched.
The reservation happens in another service, on the far side of the broker, so the order is read back a moment later:

```bash
sleep 3
curl -s localhost:8000/orders/$ORDER_ID -H "X-Entity-Id: $ENTITY_ID" | jq '{order_number, status}'
```

```json
{
  "order_number": "ORD-000004",
  "status": "RESERVED"
}
```

## Scenario 2: stock is insufficient

The same commands with `Idempotency-Key: demo-2` and a quantity of 500, against 100 units in stock.
The confirmation is answered the same way:

```json
{
  "order_number": "ORD-000005",
  "status": "CONFIRMED"
}
```

and the order read back three seconds later has been refused:

```json
{
  "order_number": "ORD-000005",
  "status": "RESERVATION_FAILED"
}
```

This is a business failure, not a technical one: the answer is final, so it is not retried and nothing reaches a dead-letter topic.
No stock was held for the order, so there is nothing to release.

## What the logs show

Each consumer logs one `event_handled` line per event.
Sorted by time, the lines for the two orders are:

```bash
docker compose logs --no-color --no-log-prefix inventory-worker order-api-consumer \
  | grep event_handled | jq -c '{timestamp, service, event_type, partition}' | sort | tail -4
```

```json
{"timestamp":"2026-09-28T07:52:40.765988Z","service":"inventory-worker","event_type":"OrderConfirmed","partition":3}
{"timestamp":"2026-09-28T07:52:42.281031Z","service":"order-api-consumer","event_type":"InventoryReserved","partition":3}
{"timestamp":"2026-09-28T08:06:37.344148Z","service":"inventory-worker","event_type":"OrderConfirmed","partition":11}
{"timestamp":"2026-09-28T08:06:38.168687Z","service":"order-api-consumer","event_type":"InventoryReservationFailed","partition":11}
```

Each order crosses the broker twice: `OrderConfirmed` to `inventory-worker`, and the outcome back to `order-api`.
The request and its reply land in the same partition number, because both topics have 12 partitions and both events are keyed by the order id.
That key is what keeps one order's events in order, as [design §4.2](design/domain-design.md) sets out.

## Notes

- The first request after `docker compose up -d` can take several seconds while connections are opened; 8 seconds was measured on a laptop.
  A later request answers in well under a second.
- The logs also carry `trace_id`, but a trace currently ends at the broker, so one order produces several unrelated traces rather than one ([TD-004](tech-debt.md)).
