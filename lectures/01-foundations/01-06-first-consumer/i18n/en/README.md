# 01-06 — First Consumer

In the previous lecture we wrote the producer. Ten records went into three partitions, the offset sum added up. Now we switch sides — writing the first consumer. The goal is the same: build a working mental model that module 03 (groups, rebalances, commits, guarantees) will layer on top of.

This is the most basic case. One consumer in a group with auto-commit enabled, graceful shutdown on SIGINT. No retry topics, no DLQ, no manual commits. That's all later.

## kgo.ConsumerGroup — why start with a group

franz-go has two consumption modes. The first is a direct consumer, without a group. You enumerate topics and partitions yourself, track offsets yourself, and store them somewhere externally. Useful for admin utilities and cases where an external database holds the position (e.g., in Kafka Streams-like pipelines). The course doesn't use this mode anywhere except the offset lectures; details are in [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/ru/README.md).

The second mode is a consumer group. This is what 99% of production code uses. The group handles:

- partition distribution among members (one topic with 6 partitions + 3 consumers = 2 partitions each);
- storing committed offsets in `__consumer_offsets` (covered in [Offsets and retention](../../../01-04-offsets-and-retention/i18n/ru/README.md));
- rebalances — when a group member joins or leaves, partitions are redistributed automatically;
- coordination via the group coordinator on the broker.

Enabled with one line:

```go
cl, _ := kgo.NewClient(
    kgo.SeedBrokers(...),
    kgo.ConsumerGroup("lecture-01-06-group"),
    kgo.ConsumeTopics("lecture-01-05-first-producer"),
)
```

`ConsumerGroup("...")` is the group.id. Kafka uses it to distinguish one logical consumer (possibly assembled from multiple processes) from another. Two processes with the same group.id are **one** group — they share partitions. Two processes with **different** group.ids are two independent groups; each has its own committed offset, and they read the same messages in parallel without interfering.

`ConsumeTopics("...")` — which topics to subscribe to. Multiple topics are supported; the list is fixed, or use `ConsumeRegex` with a pattern.

## PollFetches — how we receive messages

The consumer's main work is in a loop:

```go
for {
    fetches := cl.PollFetches(ctx)
    fetches.EachRecord(func(r *kgo.Record) {
        // process record
    })
}
```

`PollFetches` is a blocking call. It waits until at least something arrives from a broker and returns `kgo.Fetches`. This is a **list of responses from brokers**, each containing a list of **topics**, each topic containing a list of **partitions**, each partition containing a list of records. The multi-level structure exists because a single `Fetch` can arrive from multiple partitions across multiple topics — this is more efficient from a network protocol standpoint.

For application code, this hierarchy is rarely traversed manually. Convenient wrappers are available:

- `fetches.EachRecord(fn)` — iterate over all records regardless of which partition they came from.
- `fetches.EachPartition(fn)` — iterate in batches by partition. Useful when you want to collect a batch and process everything from one partition in a single transaction.
- `fetches.Records()` — collect all records into a flat slice.
- `fetches.Errors()` — per-partition error list.

A single record is `kgo.Record`. It has Topic, Partition, Offset, Key, Value, Headers, Timestamp — exactly what the broker received on produce. The consumer sees exactly what went into the log.

One important point about PollFetches: it does not block forever. It returns as soon as the context expires or the client is closed. That's why we use `runctx.New()` as the root ctx — it cancels on SIGINT, PollFetches returns with `context.Canceled` in `Errors()`, we catch it and exit the loop. No separate shutdown channels needed.

## Auto-commit and why it lies by default

Here is the nuance that half of module 03 is written for.

In franz-go (and Kafka in general), **auto-commit** is enabled by default. Every `auto.commit.interval.ms` (5 seconds by default), the client takes the current position it has definitively read up to and commits it to `__consumer_offsets`. Convenient — you write nothing, it just works.

And it's a trap. Two traps, actually.

First trap: the commit records what has been **read**. Not what has been **processed**. When we call `PollFetches` and receive a record, it is already considered "read" from auto-commit's perspective. If we crash mid-processing (writing to DB, DB goes down, process dies) — the committed offset may have already moved to or past that record. On restart, we will **not** re-read it. The message is lost for our business logic; it's still in Kafka, but that's no help to us.

Second trap: the interval. Between two auto-commits — 5 seconds. If you crash within this window, on restart you may get **duplicates** — records we processed but Kafka didn't hear about will arrive again. This is at-least-once in the bad sense — without an idempotent handler, a duplicate record will "charge money twice".

In short: auto-commit-by-default is at-most-once with a duplicate risk within a 5-second window. Not great guarantees. [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/ru/README.md) and [Processing guarantees](../../../../03-consumer/03-03-processing-guarantees/i18n/ru/README.md) cover this in detail — manual commit, MarkCommitRecords + CommitMarkedOffsets, DB dedup, and idempotent handlers.

In our teaching code, auto-commit is left enabled **intentionally** — so there's something to discuss and something to fix later. Right now it works like this:

1. PollFetches returns a batch of records.
2. We print them (no real processing).
3. In parallel, a franz-go background goroutine sends `OffsetCommit` with the current position every 5 seconds.
4. On SIGINT we call `cl.Close()`, which **additionally** triggers a final sync-commit before closing — this is part of the franz-go client lifecycle.

That's why you see the line in the output: "остановлен по сигналу, offset'ы коммитятся в Close()". Without this final commit, auto mode could lose the last few seconds of reading.

## How to shut down correctly

The pattern:

```go
ctx, cancel := runctx.New() // SIGINT/SIGTERM → ctx.Done()
defer cancel()

cl, _ := kafka.NewClient(...)
defer cl.Close() // triggers the final commit and closes connections

for {
    fetches := cl.PollFetches(ctx)
    if fetches.IsClientClosed() {
        return
    }
    if errs := fetches.Errors(); len(errs) > 0 {
        for _, e := range errs {
            if errors.Is(e.Err, context.Canceled) { return nil }
            return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
        }
    }
    // process
}
```

What you cannot skip here. First, `defer cl.Close()` — without it, the final commit won't be sent. Second, error checking — without it, context cancellation leads to an infinite loop with silent errors. Third, passing `ctx` to `PollFetches` (not ignoring it with `context.Background()`) — this is the point through which SIGINT reaches the client.

In the module 03 lectures we will add manual commit to this; the pattern stays the same — `cl.CommitUncommittedOffsets(ctx)` appears before `cl.Close()`.

## What our code does

`cmd/consumer/main.go` does three things:

1. Creates a `kgo.Client` in consumer group mode `lecture-01-06-group`, subscribed to topic `lecture-01-05-first-producer`.
2. For a fresh group (no committed offset yet), resets to earliest. Otherwise we would not see those 10 records the producer wrote **before** the consumer started — the franz-go and Kafka default is latest.
3. Loops on PollFetches and prints a `member/partition/offset/key/value/broker-ts` table. Shuts down correctly on SIGINT.

Additionally, `OnPartitionsAssigned` and `OnPartitionsRevoked` hooks are wired up — they print to stderr which partitions this process received. Useful to observe during a rebalance (see `make run-2nd` below).

Client option setup — five lines:

```go
opts := []kgo.Opt{
    kgo.ConsumerGroup(o.group),
    kgo.ConsumeTopics(o.topic),
    kgo.ClientID(fmt.Sprintf("lecture-01-06-consumer-%s", o.memberID)),
    kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        fmt.Fprintf(os.Stderr, "[member=%s] assigned: %v\n", o.memberID, m)
    }),
    kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
        fmt.Fprintf(os.Stderr, "[member=%s] revoked:  %v\n", o.memberID, m)
    }),
}
if o.fromStart {
    opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
}

cl, err := kafka.NewClient(opts...)
```

`ConsumeResetOffset(...AtStart())` is "on first group start, read from earliest". On a second run there is already a committed offset in `__consumer_offsets`, and ResetOffset has no effect.

The main loop — `PollFetches` plus printing via `EachRecord`. Along the way we check errors: `context.Canceled` is SIGINT, everything else is a real fetch failure:

```go
for {
    fetches := cl.PollFetches(ctx)
    if errs := fetches.Errors(); len(errs) > 0 {
        for _, e := range errs {
            if errors.Is(e.Err, context.Canceled) {
                fmt.Println("остановлен по сигналу, offset'ы коммитятся в Close().")
                return nil
            }
            return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
        }
    }

    fetches.EachRecord(func(r *kgo.Record) {
        fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\n",
            o.memberID, r.Partition, r.Offset,
            string(r.Key), string(r.Value),
            r.Timestamp.Format("15:04:05.000"),
        )
    })
    _ = tw.Flush()
}
```

The final commit is done by `defer cl.Close()` — without it, the last five seconds may not reach `__consumer_offsets`, and on restart those same records will arrive again.

Expected output after `make run` on a freshly created group with 10 messages across three partitions:

```
консьюмер запущен: topic="lecture-01-05-first-producer" group="lecture-01-06-group" member=1 from-start=true
читаем; Ctrl+C — выход.

[member=1] assigned: map[lecture-01-05-first-producer:[0 1 2]]

MEMBER  PARTITION  OFFSET  KEY  VALUE     BROKER-TS
1       0          0       k-0  hello-0   16:55:01.234
1       0          1       k-3  hello-3   16:55:01.253
1       0          2       k-6  hello-6   16:55:01.271
1       0          3       k-9  hello-9   16:55:01.289
1       1          0       k-2  hello-2   16:55:01.247
1       1          1       k-5  hello-5   16:55:01.265
1       1          2       k-8  hello-8   16:55:01.283
1       2          0       k-1  hello-1   16:55:01.241
1       2          1       k-4  hello-4   16:55:01.259
1       2          2       k-7  hello-7   16:55:01.277
```

A few observations on this output.

Records within a single partition are ordered by offset — 0, 1, 2, 3. This is a **Kafka guarantee** and it always holds. Between partitions, the order is completely undefined: some from partition 0, some from 1, some from 2 — the client reads them in parallel, and the assembly in the combined stream depends on `PollFetches` timing and which partition responded to the broker first. Run it a second time and the specific row order may differ. Ordering per partition is the only ordering guarantee Kafka provides. There is no global order across a topic.

Member is "1" everywhere because we have one process. All three partitions were assigned to it — the assigned map shows exactly that.

Run `make run` again with the same group.id and the table will be empty. The committed offset is already at 4/3/3 (per partition), there are no new messages, and the consumer just sits in `PollFetches` waiting. This is "committed offset working" — there is no bug here. To re-read everything from the beginning, use `make run-fresh` — it appends a random suffix to the group.id and gets a fresh group with empty offsets.

## Two consumers in one group — observing a rebalance

Run in the first terminal:

```sh
make run
```

You see that member=1 received all three partitions (`assigned: map[...:[0 1 2]]`). It finished reading and is waiting.

In the second terminal:

```sh
make run-2nd
```

A rebalance happens. The first process prints `[member=1] revoked: ...` to stderr (some partitions leave) and immediately `[member=1] assigned: ...` with the reduced list. The second process prints `[member=2] assigned: ...` with the partitions it received. The standard distribution for cooperative-sticky (the franz-go default in recent versions) is two partitions to one, one to the other. Which specific ones depends on the implementation, but with an equal number of partitions the split is usually balanced.

A rebalance is a brief pause in consumption: all group members return their partitions, the coordinator rebuilds the assignment, and members pick up the new list and resume reading. In [Groups and rebalances](../../../../03-consumer/03-01-groups-and-rebalance/i18n/ru/README.md) we cover the four rebalance assignors (range, round-robin, sticky, cooperative-sticky); also the difference between eager and incremental cooperative, and why cooperative-sticky typically has less downtime. Here — just observe that it works, and what it does is automatic load redistribution among members.

Close the second process (Ctrl+C). The first one gets `revoked` + `assigned` with all three partitions again. That's expected.

## Running

The sandbox must be running (`docker compose up -d` from the repo root). Before starting the consumer — the producer [First producer with franz-go](../../../01-05-first-producer/i18n/ru/README.md) must have already run and written at least something to the topic.

```sh
# in 01-foundations/01-05-first-producer
make run

# in 01-foundations/01-06-first-consumer
make run
```

To inspect the group's committed offset from Kafka's side:

```sh
make group-describe
```

This runs `kafka-consumer-groups.sh --describe`. It prints per-partition committed offset, lag (the difference between latest and committed), member-id, and client host. After reading all 10 messages — lag=0 on all partitions. Kill the process mid-processing and immediately run `group-describe` — you'll see lag, because auto-commit didn't make it in time.

To reset the group's committed offsets (e.g., to re-read those 10 messages from the beginning):

```sh
make group-delete
```

Deleting the group does not touch the log data — it erases the entry in `__consumer_offsets`. On the next consumer start, the group is considered "new", starting from earliest (with `from-start=true`). This is a handy technique in teaching lectures; in production don't do it without understanding what you're resetting.

## Why all this

The consumer mental model should look like this:

1. **A group is a logical consumer**. group.id is its name. The same group.id on multiple processes = partition sharing. Different group.ids = independent readers.
2. **PollFetches returns Fetches → Topics → Partitions → Records**. At the application level you almost always work via `EachRecord` or `EachPartition`.
3. **Auto-commit lies by default**. It commits the read position, which has no connection to actual processing in code. We fix it in module 03.
4. **Shutdown via cl.Close + ctx from runctx**. Without `defer cl.Close()` the last seconds of committed offset are lost; without `ctx` in PollFetches you can't exit the loop on SIGINT.

Next — module 02. Back to the producer, digging into what was "default": [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/ru/README.md), [Acks and durability](../../../../02-producer/02-02-acks-and-durability/i18n/ru/README.md), [Idempotent producer](../../../../02-producer/02-03-idempotent-producer/i18n/ru/README.md), [Batching and throughput](../../../../02-producer/02-04-batching-and-throughput/i18n/ru/README.md), [Errors, retries and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/ru/README.md).
