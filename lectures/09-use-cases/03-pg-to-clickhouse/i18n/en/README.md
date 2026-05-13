# 09-03 — Postgres → ClickHouse with anonymization

ETL for analytics. On the left: Postgres with real PII. On the right: ClickHouse, where raw personal data cannot go. Between them — Debezium (snapshot + live CDC), our Go anonymizer, and the ClickHouse Sink connector. This use case builds on CDC from [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/en/README.md), contracts from [Protobuf in Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/en/README.md), the declarative masking pattern, and ReplacingMergeTree for deduplication.

## What we're building

```
Postgres (15442)
   │
   │  WAL (logical replication, slot)
   ▼
kafka-connect: Debezium PostgresConnector
   │
   ▼
cdc.public.users / cdc.public.orders / cdc.public.events
   │
   ▼
anonymizer (Go, consumer-group)
   │  applies anonymize.yaml
   ▼
analytics.users / analytics.orders / analytics.events
   │
   ▼
kafka-connect: ClickHouse Sink
   │
   ▼
ClickHouse (18123) → ReplacingMergeTree(cdc_lsn)
```

The key difference from [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/en/README.md) is two stages instead of one. In [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/en/README.md), Debezium sent raw CDC directly to the consumer. Here, an anonymizer sits between Debezium and the Sink: it reads Debezium events, strips PII according to YAML rules, and publishes a safe version to `analytics.*`. The ClickHouse Sink knows nothing about those rules and doesn't need to — it just writes JSON into tables.

## Why a middleware instead of Connect SMT

You could do everything with Single Message Transforms inside the ClickHouse Sink. We didn't:

- different tables need different rules (hash in some places, drop in others, truncate elsewhere);
- truncating a name by locale is logic, not a regex substitution;
- the salt for hashing should be stored separately from the Connect config, not embedded as plain text in a JSON connector config;
- masking errors through SMT are hard to test — Connect has no real unit-test harness;
- the declarative YAML with rules is read and edited by non-developers.

So the rules live in a separate Go service with table-driven unit tests (see `internal/anonymizer/rules_test.go`). Connect does what it does well: reading WAL and batch-inserting into ClickHouse.

## Declarative mask config

`anonymize.yaml` describes what to do with each field. Four rules are supported: hash, drop, truncate, and passthrough (the default).

```yaml
salt: "usecase-09-03-salt"

tables:
  users:
    target_topic: analytics.users
    fields:
      email:      hash
      phone:      hash
      full_name:  truncate
      birth_date: drop      # raw date of birth — PII

  orders:
    target_topic: analytics.orders
    fields:
      notes: drop           # free-form user text
```

Fields like `id` or `created_at` don't need to be listed explicitly — without a rule, passthrough fires. This keeps the YAML from bloating with empty lines.

`hash` is SHA-256 of the salt plus the value, in hex. Deterministic: the same email with the same salt produces the same hash in every environment, so you can join on `email_hash` in analytics. The salt is in the repo because this is a sandbox; in production it lives in a secret and the YAML references it via `${ANONYMIZE_SALT}` or similar.

`truncate` formats as `{first word}{initial of second word}.`. "Ivan Ivanov" becomes "IvanI.". Useful when you want to store a low-cardinality name category without disclosure.

`drop` means the field never reaches the output payload. There is no corresponding column in the ClickHouse table, so the Sink can't accidentally surface it.

## What the code does

The key function is `transform()` in `internal/anonymizer/runtime.go`. It takes a Debezium record and decides what to send to `analytics.*`:

```go
switch env.Op {
case "d":
    raw = env.Before
    deleted = 1
case "c", "u", "r":
    raw = env.After
default:
    return nil, nil
}
```

DELETE becomes a row with `_deleted=1` (using the `before` field, because `after` is empty after a delete). INSERT, UPDATE, and snapshot replay (`r`) use `after` with `_deleted=0`. Truncate operations and unknown ops are skipped — we don't need them in analytics.

Next, the YAML rules are applied:

```go
cleaned, ok := cfg.Apply(table, raw)
if !ok {
    return nil, nil
}

cleaned["cdc_lsn"] = lsnFromSource(env.Source)
cleaned["_deleted"] = deleted
```

`Apply` lives in `internal/anonymizer/rules.go`. It switches on the rule for each field; unknown fields are copied as passthrough:

```go
switch rule {
case RuleDrop:
    continue
case RuleHash:
    out[k] = hashWithSalt(c.Salt, v)
case RuleTruncate:
    out[k] = truncateName(v)
case RulePassthrough:
    out[k] = v
}
```

`cdc_lsn` is taken from the `source.lsn` field of the Debezium event. This is the monotonically increasing 64-bit counter from Postgres — the ideal version column for ReplacingMergeTree. If LSN is missing for some reason (corrupt event), it falls back to `ts_ms`, which is good enough for analytics.

### Main loop

`Run` is a standard poll loop:

```go
fetches := cl.PollFetches(ctx)
...
fetches.EachRecord(func(r *kgo.Record) {
    rec, err := transform(r, opts.Config)
    ...
    outRecords = append(outRecords, rec)
})
if len(outRecords) > 0 {
    res := prod.ProduceSync(ctx, outRecords...)
    ...
}
if err := cl.CommitUncommittedOffsets(ctx); err != nil {
    return fmt.Errorf("commit offsets: %w", err)
}
```

At-least-once. There is a window between `ProduceSync` and `CommitUncommittedOffsets`: if the process dies, the batch is sent again. Duplicates on the ClickHouse side collapse via ReplacingMergeTree — which holds `cdc_lsn`.

## ReplacingMergeTree and cdc_lsn

In `ch/init.sql`:

```sql
CREATE TABLE IF NOT EXISTS analytics.users (
    id          Int64,
    email       String,
    phone       String,
    full_name   String,
    country     String,
    created_at  String,
    updated_at  String,
    cdc_lsn     UInt64,
    _deleted    UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_lsn)
PRIMARY KEY (id)
ORDER BY (id);
```

`ReplacingMergeTree(cdc_lsn)` means: for identical ORDER BY keys, ClickHouse keeps the row with the highest `cdc_lsn`. Merging happens in background merge jobs; for ad-hoc queries you can append `FINAL` to get the already-collapsed version (slower, but correct).

`_deleted` is a soft-delete flag. ReplacingMergeTree doesn't support hard deletes, and we don't need them: filter "deleted" rows with `WHERE _deleted=0`, and for GDPR audits it's still useful to see the event.

Timestamps are stored as String. This is a trade-off: Debezium sends `TIMESTAMPTZ` as an ISO string, and JSONEachRow without `date_time_input_format=best_effort` fails on it with code 27. You could add that setting to the Sink, but String is simpler in a teaching sandbox — no double configuration needed. In production you pay 20–30 bytes per timestamp instead of 8 and move on.

## REPLICA IDENTITY FULL

In `db/init.sql`:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

Without this, Postgres puts only the PK in the `before` field of WAL for UPDATE/DELETE. For UPDATE the anonymizer doesn't care (it only looks at `after`), but for DELETE it's critical: on a soft-delete we want to know what email the deleted user had, so the dedup by `email_hash` lines up. With `REPLICA IDENTITY FULL`, Debezium sends the full row before the change. The cost is more WAL; unnoticeable at tutorial load.

## Duplicates: where they appear and where they're suppressed

Two sources of duplicates:

1. Anonymizer recovery after a crash. There is a window between producing to `analytics.*` and committing the offset. A restart replays the batch.
2. The Debezium replication slot after a kafka-connect restart. The connector remembers the LSN up to which it committed to Kafka, but there is also an at-least-once window between WAL read and Kafka write.

Both are suppressed the same way: in ClickHouse, duplicates differ from the original only in that `cdc_lsn` is identical (or slightly lower on retry). ReplacingMergeTree merges them into one version. For extra confidence, run `OPTIMIZE TABLE ... FINAL` on a schedule.

## Snapshot + live stream

In `connectors/debezium-pg-source.json`:

```json
"snapshot.mode": "initial",
"slot.name": "usecase_09_03_slot",
"publication.name": "dbz_publication"
```

On first start the connector runs an initial snapshot: reads all rows from `users/orders/events` and sends them to `cdc.public.*` as op=`r` (read). After the snapshot it switches to reading WAL via the replication slot — every INSERT/UPDATE/DELETE arrives with op=`c`, `u`, `d`.

The anonymizer doesn't care whether it's a snapshot or live: it handles `c`, `u`, `r` the same way (takes `after`), and `d` separately (takes `before`, sets `_deleted=1`). Backfill is free: add a new table, restart the connector with a fresh slot, and the snapshot rolls through the full history.

## Integration test

`test/integration_test.go` (build tag `integration`) — full E2E. It runs the following sequence:

1. Cleans Postgres, ClickHouse, Kafka topics, consumer groups.
2. Registers Debezium and ClickHouse Sink via the Connect REST API with a unique slot name (required so a previous run with a dropped Postgres instance doesn't leave a sticky offset in `_connect-offsets`).
3. Starts the anonymizer in a goroutine.
4. Inserts 100 rows into Postgres and waits for the snapshot to complete.
5. Kills the anonymizer, inserts another 100 rows, restarts it — verifies recovery from the committed offset.
6. Waits for `>= 200` rows in each ClickHouse table, with a 4-minute deadline for users (its snapshot takes the longest).
7. Checks three anonymization invariants: email/phone are sha256-hex, full_name ends with a dot, events contain a row with `_deleted=1`.

The test requires the root sandbox to be running — `make up && make pg-init && make ch-init && make connect-install-plugins` — and plugins in `connect-plugins/`. Without them the test auto-skips via `t.Skipf` with a clear message.

There is also `test/anonymizer_test.go` without Connect: it pushes Debezium-format JSON directly into `cdc.public.*` and verifies that the anonymizer correctly delivers to `analytics.*`. Useful when Kafka is available but installing Connect is inconvenient.

## Files

- `anonymize.yaml` — declarative masking rules.
- `db/init.sql` — Postgres tables + publication for Debezium.
- `ch/init.sql` — analytics tables on ReplacingMergeTree.
- `connectors/debezium-pg-source.json` — source connector config.
- `connectors/clickhouse-sink.json` — sink connector config.
- `proto/analytics/v1/events.proto` — contracts for clean topics (on-wire JSON; the proto file documents the structure).
- `cmd/anonymizer/main.go` — thin CLI wrapper over `internal/anonymizer.Run`.
- `cmd/db-loader/main.go` — INSERT/UPDATE/DELETE generator for manual runs.
- `internal/anonymizer/runtime.go` — `Run` and `transform`.
- `internal/anonymizer/rules.go` — `Apply`, `hashWithSalt`, `truncateName`.
- `test/integration_test.go` — full E2E with Debezium + ClickHouse.
- `test/anonymizer_test.go` — pipeline-component test without Connect.
- `docker-compose.override.yml` — Postgres 15442 + ClickHouse 18123, both with healthcheck.

## Running

The root sandbox must be up (`docker compose up -d` from the root) and Connect plugins must be installed (`make -C lectures connect-install-plugins`).

```bash
make up                          # Postgres + ClickHouse
make pg-init                     # tables + publication
make ch-init                     # analytics schemas
make topic-create-all            # cdc.public.* and analytics.*
make connect-plugin-check        # verify Debezium and CH Sink are available
make connector-create-all        # register both connectors
make connector-status            # both should be RUNNING

# in a separate terminal:
make run-anonymizer

# in a third terminal — load:
make db-load DLOAD_COUNT=200
```

After a few seconds data appears in ClickHouse:

```bash
make ch-shell
> SELECT count() FROM analytics.users;
> SELECT email FROM analytics.users LIMIT 3;     -- sha256-hex
> SELECT full_name FROM analytics.users LIMIT 3; -- "User1L." etc.
```

When done, `make clean` drops the connectors, replication slots, and containers. Postgres and ClickHouse volumes are removed as well: the next run starts from a clean state.

## What's out of scope

- Schema Registry and `sr.Serde`. The ClickHouse Sink reads JsonConverter by default; Avro/Protobuf via SR requires a different converter combination. The SR wrapping task is covered in [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/en/README.md); to add it here, change `value.converter` in both connectors and wrap `prod.ProduceSync` in `sr.Serde`.
- Tombstones: `tombstones.on.delete: true` in Debezium sends an empty record after `d`. The anonymizer skips it (see `transform`, `r.Value == nil`) because `_deleted=1` was already sent with the before-frame. If the Sink needs a tombstone for compaction, change the logic.
- Backup and replay: after a real failure, Connect may complain that the replication slot does not exist. Fix it with `pg_drop_replication_slot` (the `connector-delete-all` Makefile target already does this) and recreate the connector with `snapshot.mode: initial`.
- Performance: at high volumes (millions of rows) the ClickHouse Sink starts hitting batch limits. Useful knobs are `clickhouse.bulk.size` and `consumer.override.fetch.min.bytes`. We don't touch them in the sandbox.
