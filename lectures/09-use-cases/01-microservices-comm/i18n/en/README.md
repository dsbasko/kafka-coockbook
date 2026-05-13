# Use case 09-01 — Microservices Communication

The [Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md) lecture showed the gRPC + Kafka hybrid with a single node per role. That was a concept. This is a production variant of the same hybrid in a realistic form: each service runs as multiple replicas, load is distributed across them, one replica crashes mid-load — the system keeps processing orders to completion. Everything is covered by an integration test that runs with a single command.

## What's inside

Three services:

1. `order-service` — gRPC API `OrderService.Create` plus a built-in outbox publisher. Runs as multiple replicas with different gRPC ports. All replicas write to the same Postgres, the same `outbox` table, and compete synchronously for unpublished rows via `FOR UPDATE SKIP LOCKED`. Duplicates are impossible: each row goes to exactly one node.
2. `inventory-service` — consumer on `order.created` in the `inventory` group. Also multiple replicas. Partitions are distributed across nodes via the consumer group mechanism; each message is processed by exactly one node. When a node crashes — rebalance, surviving nodes pick up its partitions.
3. `notification-service` — a second consumer on the same topic, but in its own `notifications` group. This is the exact trick Kafka was added to the hybrid for: a new downstream plugs in without touching `order-service` and without any coordination. Enable the group — read the log from the beginning — done.

One Postgres for everyone, for compactness, as in the [Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md) lecture. In production each service has its own database. One topic `usecase-09-01-order-created` with 6 partitions and RF=3.

Everything `order-service` sends to Kafka is an `OrderCreated` payload in Protobuf, plus a set of headers (`outbox-id`, `aggregate-id`, `publisher-node`, `trace-id`, `tenant-id`, `event-type`, `content-type`). Headers carry end-to-end context propagation and deduplication. Payload is proto bytes, no Schema Registry. The [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md) lecture shows how to layer SR on top; SR is omitted here so the integration test is self-contained and does not depend on schema registration over the network on every run.

## How this use case differs from the [Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md) lecture

| What | 06-04 (concept) | 09-01 (use case) |
|---|---|---|
| Nodes per service | 1 | 2+ (in the test — two for order and two for inventory) |
| Outbox publisher | in the same process as gRPC | same — but verified under contention via `FOR UPDATE SKIP LOCKED` |
| Failure recovery | not shown | in the test: kill -9 one inventory node mid-load |
| Verification | by eye in logs | integration test with asserts on 4 counters in the database |
| Event serialization | JSON in outbox payload | proto.Marshal, headers with `content-type` |
| Tracing | trace_id in headers | same, but with `publisher-node` for multi-node debugging |

## Architecture

```
            ┌────────────────┐         ┌────────────────┐
            │   gRPC client  │────────►│  order-service │
            └────────────────┘  Create │     :50081     │  ───┐
            ┌────────────────┐         │  (node order-1)│     │
            │   gRPC client  │────────►│                │     │
            └────────────────┘  Create └────────────────┘     │
                                       ┌────────────────┐     │  Postgres TX:
                                       │  order-service │     │  orders + outbox
                                       │     :50082     │     │  ↑
                                       │  (node order-2)│ ◄───┘
                                       └────────────────┘
                                                │
                                  outbox publisher (goroutine in each replica)
                                                │
                                                ▼
                                  ┌────────────────────────┐
                                  │   Kafka topic           │
                                  │   order.created (P=6)   │
                                  └────────────────────────┘
                                                │
                          ┌─────────────────────┴─────────────────────┐
                          │                                           │
                  consumer group                                consumer group
                  "inventory"                                   "notifications"
                          │                                           │
                ┌─────────┴─────────┐                          ┌──────┴──────┐
                ▼                   ▼                          ▼
        ┌─────────────┐    ┌─────────────┐            ┌─────────────────┐
        │ inventory-1 │    │ inventory-2 │            │ notification-1  │
        └─────────────┘    └─────────────┘            └─────────────────┘
                │                   │                          │
                └─────────┬─────────┘                          │
                          ▼                                    ▼
                  inventory_reservations                notifications_log
```

The left half is the write-path. No `Produce` inside the RPC handler: only `INSERT INTO orders` plus `INSERT INTO outbox` under a single `COMMIT`. That is the outbox pattern contract (see [Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md) and [Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md)). The outbox publisher is a separate goroutine in the same process. On each tick it polls `SELECT ... WHERE published_at IS NULL ... FOR UPDATE SKIP LOCKED`, sends a batch to Kafka, and marks rows `published_at = NOW()`. If two `order-service` replicas poll simultaneously — `SKIP LOCKED` prevents them from grabbing the same row; no leader election, no node-level sharding needed.

The right half is two independent consumers. `inventory` has multiple nodes in one group, partitions are split. `notifications` has one node in its own group, reads all partitions. No fundamental difference: the same topic is consumed by two independent projections that know nothing about each other.

## How inventory crashes and what happens next

The scenario the integration test verifies: midway through the load (after ~100 of 200 orders) `inventory-1` is stopped. Before the stop:

```
inventory-1 owns partitions [0, 2, 4]
inventory-2 owns partitions [1, 3, 5]
```

After stopping one node:

```
inventory-2 owns partitions [0, 1, 2, 3, 4, 5]   ← after rebalance
```

Partitions `[0, 2, 4]` did not go unhandled — Kafka detected the departure via session timeout (15 seconds), triggered rebalance, and transferred them to the only live node. Messages that `inventory-1` had already received but not committed will be redelivered to `inventory-2`. Duplicates are caught at the dedup table level: `processed_events (consumer, outbox_id)` with `ON CONFLICT DO NOTHING` in `inventory_reservations`.

In the test output this looks like:

```
[inventory-1] inventory-service остановлен. processed=N reserved=N skipped=0
counts: orders=200 outbox_unpublished=0 reservations=87  notifications=200
counts: orders=200 outbox_unpublished=0 reservations=163 notifications=200
counts: orders=200 outbox_unpublished=0 reservations=200 notifications=200
inventory распределение: map[inventory-2:200]
```

First reservations=87 (what was processed before the kill), then a plateau (rebalance — a few seconds pause), then the number climbs to 200. Notifications did not blink — they have their own group, no rebalance occurred.

## Key code

`order-service` and `inventory-service` are implemented as thin `cmd/<name>/main.go` plus `internal/<name>service` packages with an exported `Run(ctx, opts) error` function. This is required by the integration test: it imports `internal/orderservice`, `internal/inventoryservice`, `internal/notificationservice` directly and runs them as goroutines in a single process. Without this, real processes would have to be spawned via `os/exec` and communicated with through PID files — unnecessary noise for a test scenario.

The order-service transaction itself:

```go
err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, insertOrderSQL,
        id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(),
        ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    ); err != nil {
        return fmt.Errorf("INSERT orders: %w", err)
    }
    aggregateID := "order-" + id
    if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, payload).Scan(&outboxID); err != nil {
        return fmt.Errorf("INSERT outbox: %w", err)
    }
    return nil
})
```

Both INSERTs are in `tx` under a single `COMMIT`. No `cl.ProduceSync(...)` here. If `COMMIT` fails — the client sees Internal, retries, enters a new transaction. If `COMMIT` succeeds but the process dies before the outbox publisher sends the record — it stays in `outbox` with `published_at IS NULL` and is published when any node restarts.

The publisher itself — bare `FOR UPDATE SKIP LOCKED`:

```go
const fetchBatchSQL = `
SELECT id, aggregate_id, topic, payload
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED
`
```

This is the entire secret behind contention between multiple `order-service` replicas. Postgres guarantees that rows selected by one transaction will not appear in another until the first commits or rolls back. `SKIP LOCKED` means "if a row is locked — skip it, don't wait." Two nodes enter the `SELECT` simultaneously, each gets its own set of rows, neither blocks the other.

Records in Kafka are assembled with headers:

```go
records[i] = &kgo.Record{
    Topic: r.topic,
    Key:   []byte(r.aggregateID),
    Value: r.payload,
    Headers: []kgo.RecordHeader{
        {Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
        {Key: "aggregate-id", Value: []byte(r.aggregateID)},
        {Key: "publisher-node", Value: []byte(o.NodeID)},
        {Key: "trace-id", Value: []byte(evt.GetTraceId())},
        {Key: "tenant-id", Value: []byte(evt.GetTenantId())},
        {Key: "event-type", Value: []byte("order.created")},
        {Key: "content-type", Value: []byte("application/x-protobuf")},
    },
}
```

`aggregate-id` goes into `Key` — all events for the same order go to the same partition, ordering is preserved per-key. `outbox-id` is the deduplication hook on the consumer side: each `order-service` has its own autoincrement in Postgres (the same `BIGSERIAL` in the shared table, which is even simpler), and the pair (consumer, outbox_id) is unique. `publisher-node` is for debugging: it shows which `order-service` node published the record. Useful when reading kafka-logs in production and trying to identify the author instance.

Deduplication on the consumer side:

```go
tag, err := pool.Exec(ctx, dedupSQL, consumerName, outboxID)
if err != nil { return err }
if tag.RowsAffected() == 0 {
    skipped.Add(1)
    continue
}
```

`dedupSQL` is `INSERT INTO processed_events (consumer, outbox_id) VALUES ($1, $2) ON CONFLICT (consumer, outbox_id) DO NOTHING`. If a row already exists — `RowsAffected()` returns 0, processing is skipped. This is equivalent to `SELECT 1 FROM processed_events ... + INSERT`, but in a single SQL statement with no race condition between SELECT and INSERT.

## Integration test

The test is in `test/integration_test.go`. What it does:

1. Pings Postgres and Kafka. If either does not respond — `t.Skip()`. The test depends on external infrastructure by design: the lecture is about how this code works on a real sandbox.
2. Clears all tables (`TRUNCATE`) and recreates the topic from scratch. This matters — otherwise stale offsets from previous runs will pull consumers to an unexpected position.
3. Deletes existing consumer groups (`adm.DeleteGroup`). Also about run idempotency.
4. Starts 5 nodes as goroutines: 2 × `order-service`, 2 × `inventory-service`, 1 × `notification-service`. Each has its own `context.WithCancel(root)` — so one can be killed without touching the others.
5. Sends 200 `Create` requests over gRPC round-robin between two `order-service` nodes. At the midpoint (i=100) it calls `inv1.cancel()` and waits for `<-inv1.done`. This is a synchronous stop.
6. Continues sending the remaining 100 orders. During this time rebalance is in progress — `inventory-2` takes a couple of seconds to pick up partitions.
7. Then polling: every 500ms reads `COUNT(*)` from four tables. Waits until all four equal 200. Deadline — 60 seconds.
8. Final check — `inventory-2` must have processed at least some records (`map[inventory-2:N], N > 0`). If it processed nothing — recovery did not work, the test fails.

Run:

```sh
make up                 # postgres
make db-init            # create tables
make test-integration   # go test ./test/... -v -count=1 -timeout=180s
```

Full cycle — about 30 seconds on my sandbox. Most of it is waiting for rebalance (15-second session timeout) and reservations catching up.

## Manual run

For debugging or live demonstration — everything is broken into Makefile targets. In separate terminals:

```sh
# terminal 1
make up && make db-init
make topic-create

# terminal 2
make run-order-1

# terminal 3
make run-order-2

# terminal 4
make run-inventory-1

# terminal 5
make run-inventory-2

# terminal 6
make run-notification

# terminal 7
make grpcurl-create     # send a test Create
make orders-count       # incremented by 1
make reservations-count # should also increment (with a lag)
make notifications-count
```

Then `Ctrl+C` any `inventory-N` and watch the surviving one pick up the partitions. After 15 seconds of session timeout (logs will show rebalance) processing resumes.

## What is intentionally simplified in this use case

- **One Postgres for everyone.** In production each service has its own database. Here it is shared for code and test compactness. The logic does not change: the dedup table is still per-consumer, the write-path is still atomic per-service.
- **No Schema Registry.** Events in Kafka are raw protobuf bytes. SR is shown in the [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md) lecture and layers on top via `sr.Serde`. Omitted here: the test must be fast and self-contained.
- **No real-world load patterns.** 200 orders via round-robin on two nodes is a unit-stress test, not a load test. Throughput benchmarks are not the goal of this lecture.
- **Mock notification.** `notification-service` writes to `notifications_log` instead of real delivery. Real channels (Firebase / APNs / webhook with circuit breaker and retry) are covered in the next use case [Push notifications](../../../02-push-notifications/i18n/en/README.md).

What is not simplified — the outbox publisher with `FOR UPDATE SKIP LOCKED` on multiple nodes, multi-node consumer group with recovery, consumer-side deduplication via `processed_events`. This works exactly as in production.

## Links to other lectures

- **[Outbox pattern](../../../../04-reliability/04-03-outbox-pattern/i18n/en/README.md)** — the outbox idea itself. Here it simply scales to N nodes.
- **[Hybrid gRPC + Kafka](../../../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/en/README.md)** — the hybrid concept. This use case is its production edition.
- **[Groups and rebalancing](../../../../03-consumer/03-01-groups-and-rebalance/i18n/en/README.md)** — what rebalance is, what session timeout is, why `cooperative-sticky` beats `range`.
- **[Processing guarantees](../../../../03-consumer/03-03-processing-guarantees/i18n/en/README.md)** — why deduplication by `(consumer, outbox_id)` turns at-least-once into effectively-once.
- **[Protobuf in Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/en/README.md)** — generating Go code via `buf`, which is used here.
