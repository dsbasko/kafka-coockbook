# 04-03 — Outbox Pattern

The previous lecture covered an exactly-once chain between Kafka and Kafka — read from a topic, write to another, commit the offset, all inside one transaction. This is a different situation. We have a database and Kafka, and we need every order created in `orders` to produce an `order.created` event in Kafka. These two stores know nothing about each other. There are no transactions spanning both.

This is exactly where people have been stepping on the same rake for years.

## Two honest approaches — both break

Approach one, naive: write to Kafka first, then Postgres. If the process dies between the two steps, the event is already in Kafka but the order is not in the database. On restart the service has no memory of what it wrote. The order never gets created, yet consumers have already been notified it exists. A phantom order begins — one for which data will never exist. Inventory reserved the item, billing is waiting for payment, support starts receiving calls.

Approach two: Postgres first, then Kafka. The order lands in the database, COMMIT goes through, the process dies between that and `Produce`. On restart nobody remembers that an event needed to be published. The order exists, the event does not. Inventory reserved nothing, billing charged nothing, the customer sees "accepted" but nothing moves forward.

The third path is some XA protocol on top of two-phase commit. It exists, but almost nobody uses it. Too fragile, too slow, operationally expensive. Most databases and brokers support it half-heartedly, and any specific pairing turns into a six-month project. Close that topic and move on.

## The outbox idea

The core trick is almost trivially simple. We place "need to publish event X" inside the database itself, in the same transaction as the order. A new table appears — `outbox`. It holds what needs to be sent and a "sent / not sent" flag. When an order is created, a single Postgres transaction runs two INSERTs — into `orders` and into `outbox`. Postgres itself provides the atomicity.

```sql
CREATE TABLE outbox (
    id            BIGSERIAL    PRIMARY KEY,
    aggregate_id  TEXT         NOT NULL,
    topic         TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ  NULL
);
```

A separate process (the publisher) then reads from this table, sends to Kafka, and marks each row as sent. If it crashes, on restart it simply continues from where it left off: a `WHERE published_at IS NULL` query picks up everything that did not make it out. The two systems stay in sync through the outbox: "everything in outbox will eventually appear in Kafka."

The price is one thing: duplicates in Kafka are possible. The specific mechanism is the window between "sent to Kafka" and "marked published_at." If the process crashes right there, on restart it sends the same message again. We live with this and protect against it on the consumer side.

## Writing from the service

There is not much code here — the main requirement is that both INSERTs are inside the same `BeginFunc`. One Postgres transaction per order:

```go
err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if err := tx.QueryRow(ctx, insertOrderSQL, customerID, amount).Scan(&orderID); err != nil {
        return fmt.Errorf("INSERT orders: %w", err)
    }

    payload, _ := json.Marshal(orderEvent{...})
    aggregateID := fmt.Sprintf("order-%d", orderID)

    if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, topic, string(payload)).Scan(&outboxID); err != nil {
        return fmt.Errorf("INSERT outbox: %w", err)
    }
    return nil
})
```

If the first INSERT fails — no order, no outbox row. If the second fails — Postgres rolls back both. If COMMIT goes through — both INSERTs become visible simultaneously. No half-states.

I put `aggregate_id` in a separate column and also in the payload. It goes into the `Key` of the Kafka message — so all events for one order land in the same partition and ordering is preserved.

## Publisher

Next: a separate process that reads from the outbox and sends to Kafka. Logic on each iteration:

1. Open a transaction.
2. `SELECT id, aggregate_id, topic, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED`.
3. `Produce` each row.
4. `UPDATE outbox SET published_at = NOW() WHERE id = ANY($1)`.
5. `COMMIT`.

`FOR UPDATE SKIP LOCKED` is a must for horizontal publisher scaling. Without it, two processes block each other hard: the first takes rows under a lock until COMMIT, the second waits, everything funnels into one thread. With SKIP LOCKED, the second process simply skips locked rows and picks up the next ones. Parallelism without coordination.

```go
const fetchBatchSQL = `
SELECT id, aggregate_id, topic, payload::text
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED
`
```

The publish loop itself:

```go
results := cl.ProduceSync(ctx, records...)
if err := results.FirstErr(); err != nil {
    return 0, fmt.Errorf("ProduceSync: %w", err)
}
// ... this is the crash window — records are in Kafka, UPDATE not yet in the database ...
tag, err := tx.Exec(ctx, markPublishedSQL, ids)
if err != nil {
    return 0, fmt.Errorf("UPDATE outbox: %w", err)
}
if err := tx.Commit(ctx); err != nil {
    return 0, fmt.Errorf("Commit: %w", err)
}
```

The idempotent producer (the default in franz-go) protects against duplicates within one session — retried `Produce` calls do not duplicate records on the broker. It does not protect against the cross-session duplicate: a new process gets a new producer ID, and the broker treats it as a different client.

On ordering: I mark rows `published_at = NOW()` in the same transaction as the SELECT. If someone prefers the UPDATE to succeed before `Produce` (optimistically) — that is fine, but then a failed Produce requires rolling back the UPDATE, which is another opportunity to land in a half-state. Let Produce come first and UPDATE come last before COMMIT. In this order the half-state is always the same one: "already published to Kafka, not yet marked in the database." One recovery scenario, well understood.

## Duplicates and consumer-side protection

Since the publisher delivers at-least-once, the consumer must survive that. The standard technique is a dedup table keyed on the event's stable ID. That ID already exists: `outbox.id` — stable, monotonically increasing, guaranteed unique. Put it in a message header:

```go
records[i] = &kgo.Record{
    Topic: r.topic,
    Key:   []byte(r.aggregateID),
    Value: []byte(r.payload),
    Headers: []kgo.RecordHeader{
        {Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
        {Key: "aggregate-id", Value: []byte(r.aggregateID)},
    },
}
```

The consumer reads the header and does a simple INSERT into its dedup table:

```sql
INSERT INTO processed_outbox_ids (outbox_id) VALUES ($1)
ON CONFLICT (outbox_id) DO NOTHING
```

`RowsAffected = 1` — new event, process it. `0` — already seen, skip silently. Simple, reliable, requires only a PRIMARY KEY on the `outbox_id` column.

```go
tag, err := pool.Exec(ctx, dedupSQL, outboxID)
// ...
if tag.RowsAffected() == 1 {
    inserted.Add(1)
    // main processing here: update inventory, call billing, ...
} else {
    skipped.Add(1)
}
```

The subtle point is where exactly to do the INSERT. I do it before the main processing: "I am claiming I will handle this event." If I crash between the INSERT and the actual work, on restart the event will be dedup'd as "already processed" and the work is lost. In practice, the fix is making the actual work idempotent too (e.g., also via ON CONFLICT in its own table keyed on the same `outbox_id`). Then "duplicate protection" and "actual processing" merge into one operation and the problem disappears. In this lecture I split them for clarity.

## Polling vs CDC

What we wrote is the polling variant. Every N milliseconds the publisher goes to the database and asks "anything new?" Simple, understandable, works well at moderate load (tens of thousands of events per second is easy). The downside: latency between "event in outbox" and "event in Kafka" depends on poll frequency. You can tighten it to 100ms or lower; eventually you start catching an empty query on every tick.

The alternative is a CDC publisher. Instead of polling the table it listens to Postgres logical replication and sees INSERTs into outbox in real time. That is Debezium territory, covered separately in [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/en/README.md). The key difference in guarantees: CDC gives lower latency and moves the "coordination" out of the database into the replica, but in return requires `wal_level=logical`, a dedicated replication slot, and a persistent process next to the database. At the application model level the outbox table remains the same: write to `orders` plus `outbox` in one TX, and something reads from `outbox` and sends to Kafka. Only that "something" changes.

A third variant, found in small services, is a publisher that lives in the same process as the order service and shares the same pool. After each INSERT into outbox it fires an in-code event trigger: "go publish this right now." Latency drops, but it has scaling problems across N instances and does not survive a service restart (unless the same poller runs at startup). I will not demonstrate this variant in code — for understanding the pattern, the "separate publisher" model matters more.

## Demo

Bring up the database, apply the schema, create the topic:

```sh
make up
make db-init
make topic-create
```

Create 20 orders:

```sh
make run-service COUNT=20
```

In Postgres we see 20 rows in `orders` and 20 in `outbox`, all with `published_at IS NULL`:

```sh
make db-count          # 20
make outbox-count      # 20
make outbox-pending    # 20
```

Now run the publisher with a simulated crash — it will send the first batch to Kafka and die before marking `published_at`:

```sh
make chaos-kill-publisher BATCH_SIZE=5
```

The output shows five `PUB` lines and a CRASH message. Five records went to Kafka; in the outbox they are still "unpublished." Run the normal publisher — it picks up the remainder (including those five the crashing publisher never marked) and publishes everything:

```sh
make run-publisher
```

Check that all 20 outbox rows are now marked published:

```sh
make outbox-pending    # 0
```

Now the consumer:

```sh
make run-consumer
```

The output shows 25 events: 20 unique (`INSERT`) and 5 duplicates from the first batch (`DUP`). Final stats — `processed=25 inserted=20 skipped=5`. On the consumer side: 20 actually processed and 5 silently swallowed duplicates.

Count them:

```sh
make dup-count         # 20 — unique events processed
```

What happened end to end: 20 orders, 25 messages sent to Kafka (5 duplicates from the crash), dedup on the consumer collapsed the duplicates, business logic ran 20 times. The system's guarantee — at-least-once on the producer side plus idempotency on the consumer side — gives effectively-once in the observed effect.

## What outbox does not cover

Outbox is about the "one service's database ↔ Kafka" link. External parties — HTTP calls to a third-party API, sending email, calling an SMS provider — are outside this scheme. If a consumer processes an event, calls an external API, and then commits the offset, the same "crashed between two steps" problem is there, and outbox does not fix it. The fix is idempotency on the external recipient's side (an idempotency key in the HTTP request), or another outbox layer on the consumer side ("I recorded the intent to call the API, and a separate process makes the call"). That is covered in the [Delivery to external systems](../../../04-05-external-delivery/i18n/en/README.md) lecture.

One more thing: outbox ties Kafka to the lifecycle of our database. If the database is down, the publisher cannot read. In practice this is fine — if the primary database is down, we have bigger problems than Kafka. But architecturally it is worth understanding that we made Postgres the single point of synchronization.

And finally: table size. Published rows accumulate in `outbox`, and if nothing is done, after a year there will be billions of them. The fix is partitioning by date plus a separate job to delete published rows older than 7 days. That is operations work, not part of the pattern, but do not forget it in production.

## Full run

```sh
make up
make db-init
make topic-create

make run-service COUNT=100             # 100 orders in the database
make chaos-kill-publisher BATCH_SIZE=20 # crashes after the first batch
make run-publisher                      # processes the remainder
make run-consumer                       # deduplicates duplicates

make outbox-pending                     # 0
make dup-count                          # 100 (unique events)
make end-offsets                        # partition sum > 100 (includes duplicates)
```

As a sanity check, `make outbox-count` (always exactly as many as there were orders) and `make dup-count` (the same number) are worth looking at. The difference between "end-offsets" and "number of unique events" is the cost of the outbox publisher's at-least-once guarantee.
