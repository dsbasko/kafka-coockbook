# 03-05 — Concurrency & Lag

In the previous lesson we learned to catch errors and route them to DLQ and retry chains. The business handler was mocked there, with `time.Sleep(50ms)` — and each message cost roughly that much. One consumer, one thread, one `for r := range batch`. When message volume is low — fine. Then this happens.

Say `payments` suddenly receives 5,000 messages per second. Processing one takes 10 milliseconds. One thread handles 100 per second. Lag grows linearly. After a minute we are 5 minutes behind; after an hour — an hour behind. The pipeline hasn't failed; it's just slow. From a monitoring perspective this is a "healthy but hopelessly lagging" consumer. The only worse state is "down".

Parallelize processing. Sounds simple. In practice an easy-to-forget constraint surfaces immediately.

## The only ordering guarantee is per-partition

Within a single partition, records are written to the log and read in exactly the order they were written. Across partitions — ordering is not guaranteed at all. Want events for the same user processed sequentially? Put the `user_id` as the key — all records for that key land in one partition. There are no other guarantees here.

This means the following. If you simply spin up a pool of 16 goroutines and round-robin records into them — events for the same user can land in different goroutines, and which one finishes first is unknown in advance. The `-100` debit gets processed before the `+100` credit. The balance goes negative, support gets a ticket. The pipeline is now fast and broken.

Two clean ways to scale without sacrificing ordering:

1. **More partitions + more consumers in the group.** Each consumer takes several partitions and processes each one sequentially. Simple and reliable, but the ceiling is the partition count — and you can't raise it forever (topic metadata grows, rebalances get heavier). And partitions cannot be reduced.
2. **Per-key worker pool inside a single consumer.** A goroutine pool where a hash of the key selects the worker. The same key always goes to the same worker; different keys run in parallel. One consumer serves the same partition, but processes it in parallel across keys.

This lesson is about the second approach. The first is standard and has no gotchas other than "don't inflate partitions to infinity." The second has gotchas.

## Per-key worker pool

The idea: `PollFetches` returns a batch of records. Stop processing them directly — distribute them across worker channels. A worker is a goroutine with its own `chan *kgo.Record`. Worker selection: `hash(record.Key) % N`, where N is the pool size.

```go
func workerFor(key []byte, n int) int {
    if len(key) == 0 {
        return 0
    }
    h := fnv.New32a()
    _, _ = h.Write(key)
    return int(h.Sum32() % uint32(n))
}
```

`fnv32a` is good because it's deterministic and distributes evenly across typical `user_id` and UUID values. Keyless records are trickier — ordering for them is undefined anyway, so we send them to worker 0; if you want to spread the load you can use round-robin instead (the distinction doesn't matter for this lesson).

Workers run in a simple loop:

```go
for r := range ch {
    time.Sleep(o.workDelay)
    tr.markDone(r.Topic, r.Partition, r.Offset)
    processed.Add(1)
}
```

And here is the trap.

## Out-of-order completion

Workers finish records in arbitrary order. Worker[3] may finish offset=12 faster than worker[1] finishes offset=10 (records in the same partition with different keys went to different workers). If at that moment you commit "up to the highest processed offset" — offset=12 — then crash, Kafka on restart will deliver everything starting from offset=13. Record 10 is lost.

So you can only commit at the "lower continuous boundary": the maximum offset O such that all records `<= O` have been processed. Between that and what was processed out-of-order lies a window of "pending" offsets waiting for the stragglers.

This is implemented with a per-partition tracker. The idea — two fields:

- `next` — the next offset we will resume reading from after a restart (i.e., "not yet processed");
- `done` — a set of offsets above `next` that are already complete.

The `markDone` logic: add the offset to `done`, then loop — while `done` contains `next`, remove it from `done` and increment `next`. This advances the continuous band.

```go
pt.done[offset] = struct{}{}
for {
    if _, ok := pt.done[pt.next]; !ok {
        break
    }
    delete(pt.done, pt.next)
    pt.next++
}
```

If done = {10, 11, 13} and `next` = 10 — after the loop `next` becomes 12 (12 hasn't arrived yet, the loop stops). You can commit offset=12. When 12 arrives — `next` jumps to 14. Record 13 is neither lost nor prematurely committed.

Keep this structure under a mutex. Workers write in parallel; the commit loop reads a snapshot. The lock is short; throughput impact is negligible.

## Commit separate from processing

Since workers and the tracker live in different goroutines, the manual commit naturally belongs in its own ticker. Every `commit-interval` (500 ms by default), the tracker produces a snapshot — something like `{topic: {partition: EpochOffset{...}}}` — and we call `cl.CommitOffsetsSync`. If nothing advanced during that interval — we re-commit the same offset, which is harmless.

```go
cl.CommitOffsetsSync(ctx, snap, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
    if err != nil && !errors.Is(err, context.Canceled) {
        fmt.Fprintf(os.Stderr, "commit error: %v\n", err)
    }
})
```

On Ctrl+C you cannot just close the client. The shutdown sequence:

1. Stop the dispatcher — no more records pushed into worker channels.
2. Close the worker channels.
3. Wait for workers to finish what they already have in hand.
4. Stop the commit ticker and do a final sync commit.
5. Only then call `cl.Close()`.

Otherwise you commit an offset before workers finish processing it — those same duplicates on the next start.

## Lag

Consumer lag is the difference between the log end offset (LEO) of a partition and the committed offset of the group. How many records are sitting in Kafka waiting to be read.

```
lag = LEO - committed
```

If lag = 0 — you've caught up to the head of the log. If lag is growing — processing can't keep up with the producer. If lag grows steadily (a straight upward line on the graph) — consumer throughput is lower than producer throughput; this is where you need to scale.

Lag is measured per-partition and then aggregated. Skew indicates a hot key or hot partition: overall lag is fine, but one partition is a million behind. Adding workers won't help — fix it in the keys (see the [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md) lesson and the runbook in [Troubleshooting runbook](../../../../08-operations/08-04-troubleshooting-runbook/i18n/en/README.md)).

## kadm.Lag in Go

franz-go has everything wired up: `kadm.Client.Lag(ctx, group)`. Returns `DescribedGroupLags` — a map by group name, containing per-topic per-partition `GroupMemberLag`. Convenient because under the hood it computes: DescribeGroups + FetchOffsets + ListEndOffsets + comparison. No need to wire up three requests yourself.

Our `lag-watcher` is a short ticker loop:

```go
lags, err := admin.Lag(reqCtx, group)
if err != nil {
    return fmt.Errorf("admin.Lag: %w", err)
}
dl, ok := lags[group]
if !ok {
    fmt.Printf("[%s] group=%q: not described\n", now, group)
    return nil
}
```

Then it iterates over `dl.Lag.Sorted()` — that's `[]GroupMemberLag` with fields Topic, Partition, Commit, End, Lag. Printed as `lecture-03-05-events/2=147 (commit=8500 end=8647)` — you see the partition, current lag, and the two numbers it was calculated from.

## What the code does

Three binaries plus a seed.

`cmd/sequential/main.go` — baseline. One thread, `time.Sleep(work-delay)` per record, manual commit after each batch. Prints rate once per second. This is the "healthy but slow" consumer where lag growth will be visible.

`cmd/concurrent-pool/main.go` — worker pool with a tracker. Starts N goroutines, each on its own channel. The dispatcher routes by key hash. Async commit every 500 ms. On the same topic with the same work-delay it should show `N×` throughput, provided there are at least N unique keys (otherwise workers pile up on the same channel).

`cmd/lag-watcher/main.go` — a separate process. Polls `kadm.Lag` every 2 seconds and prints per-partition lag. Group selected via flag: `-group=lecture-03-05-sequential` or `-group=lecture-03-05-pool`.

`cmd/seed/main.go` — load generator. Sends N records with K distinct keys via franz-go async producer with lz4 compression. On a local sandbox it produces tens of thousands of messages per second — more than sequential can consume, and enough for the pool to ramp up.

The commit snapshot from the tracker — what separates the out-of-order pool from the standard `CommitRecords`:

```go
out := make(map[string]map[int32]kgo.EpochOffset, len(t.parts))
for tp, pt := range t.parts {
    topic := out[tp.topic]
    if topic == nil {
        topic = make(map[int32]kgo.EpochOffset)
        out[tp.topic] = topic
    }
    topic[tp.partition] = kgo.EpochOffset{Epoch: pt.epoch, Offset: pt.next}
}
```

`pt.next` is exactly the "lower continuous boundary plus one" — the value Kafka expects in an OffsetCommitRequest as "resume from here after restart". The epoch is taken from the last record seen in the partition — needed for fencing on leader change; setting it to `-1` will work but skips the check.

## Tradeoffs

The pool with a tracker is a powerful tool, but not free.

**Memory.** If a batch of 10,000 records arrives and workers are lagging, up to 10,000 offsets can sit in the `done` sets. Until `tracker.next` catches up — they occupy memory. During a prolonged stall of one worker (e.g., a slow downstream), `done` can grow unreasonably large. The protection: a buffered channel with a reasonable size (1024 per worker here) and backpressure through `PollFetches` — when the channel is full, the dispatcher blocks on send, naturally throttling fetches.

**Rebalance.** When a partition is revoked, its `done` set may contain processed offsets above `next`. If `next` is lower — we processed them but didn't commit; the new owner of that partition will start reading from committed = `next`, and those higher processed offsets will be re-read. This is at-least-once semantics in pure form, but the window of potential duplicates on rebalance is larger than for sequential (where the window is 1 batch). In `OnPartitionsRevoked` we drop the tracker for revoked partitions to avoid confusion on reassignment; doing a final commit before the drop is optional and a separate procedure.

**Ordering — per-key only.** Different keys in the same partition can be processed in any order inside the pool. If business logic depends not only on per-key order but on "events in a partition in general" — the pool won't work. Then either use more partitions (fall back to sequential per-partition), or a stricter pool where records are routed by `(partition, hash(key))` — slightly more expensive, but preserves per-partition ordering at the tails.

**Hot key.** If one key is much more active than all others — its worker becomes the bottleneck while the rest sit idle. Symptom: lag is growing, CPU is 90% free. Fixed with a composite key (see the runbook [Troubleshooting runbook](../../../../08-operations/08-04-troubleshooting-runbook/i18n/en/README.md)) or per-record sharding with sub-keys.

**Async commit can lose a race with process crash.** Every 500 ms means 0–500 ms of processed-by-workers-but-not-committed history that will be re-read on restart. Want a smaller window — commit more often; more frequent commits — higher load on the coordinator. Standard tradeoff.

## Running it manually

You need several terminals. One for seed (sending records). A second for sequential or pool (processing). A third for lag-watcher (showing the gap).

```sh
make topic-create          # 6 partitions (sequential parallelism ceiling = 6)
make seed-fast             # SEED_MESSAGES=100000 SEED_KEYS=32 by default
```

Then in one terminal:

```sh
make run-seq               # work-delay=10ms, one thread → ~100 msg/sec
```

In another:

```sh
make run-lag               # LAG_GROUP=lecture-03-05-sequential by default
```

After a minute the lag-watcher shows per-partition lag growing linearly, total in the tens of thousands. Stop seq (Ctrl+C), start the pool:

```sh
make group-delete-all      # so the pool reads from earliest and doesn't inherit sequential's commits
make seed-fast             # send more if needed
make run-pool              # workers=8 by default
```

The pool with the same work-delay should give ~800 msg/sec (8 workers × 10ms). Lag-watcher for the pool group:

```sh
make run-lag LAG_GROUP=lecture-03-05-pool
```

Lag first rises (accumulated before startup), then starts falling. If at your settings the seed produces faster than the pool processes — lag still grows, but slower than with sequential.

To verify the tracker specifically — set `WORKERS=1` on the pool. You get sequential behavior, but through a channel. Lag will be the same. Set `WORKERS=16` and `WORK_DELAY=50ms` — ideally 320 msg/sec, in practice bounded by number of unique keys × CPU; if there are few keys, most workers sit idle.

## More experiments

- Set `SEED_KEYS=4` and `WORKERS=16`. Most workers will be idle — parallelism is bounded by the number of unique keys. On the metric this looks like "total throughput almost the same as sequential, even with 16× more workers."
- Set `SEED_KEYS=1`. The pool becomes sequential — all records go to one worker. On the graph — the same line as `cmd/sequential`.
- Raise `WORK_DELAY=100ms` on both. Sequential: 10 msg/sec, pool with 8 workers: 80 msg/sec. The formula is visible: throughput = workers / work-delay, bounded only by key distribution.
- Start the pool in two copies with the same group `lecture-03-05-pool`. Rebalance splits partitions between them. Each process runs its own pool of 8 workers. Total parallelism — up to 16 (if there are enough keys).
- Kill the pool with Ctrl+C mid-run. Restart. Use `make group-describe-pool` to see committed offsets; they should be strictly below LEO. In the worst case — between committed and LEO are "pending done" offsets — the ones re-read on restart.
- Change `COMMIT_INTERVAL=2s`. The loss window on kill grows, but coordinator load drops. On heavily loaded groups commit load is real.

## Next

The out-of-order tracker is what Kafka Streams and Flink do under the hood under the name "watermark / barrier." The idea is the same — advance the commit cursor along the lower continuous boundary. In [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md) this same tracker becomes part of consume-process-produce: the offset commit and the produces participate in a single transaction, and there are no duplicates on restart at all. More expensive, not for every case.

The [Consume-process-produce](../../../../04-reliability/04-02-consume-process-produce/i18n/en/README.md) and [Retry and DLQ deep dive](../../../../04-reliability/04-04-retry-and-dlq/i18n/en/README.md) lessons build on the fact that we separated processing from commit here. With transactions one more step is added — `SendOffsetsToTransaction`, which atomically ties "processed and written to downstream" to "committed the source offset."

If the pool with per-key sharding stops keeping up — go back to the first option: more partitions. That's fine. The pool is an optimization on top of Kafka partitioning, not a replacement for it.
