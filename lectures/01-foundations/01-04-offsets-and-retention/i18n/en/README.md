# 01-04 — Offsets and Retention

The previous lecture covered replication — how a message reaches disk and how many copies exist. This one shifts perspective. The message is on disk. How long does it stay there? And how does a client know where it is in the log?

Both questions are answered by offset and retention. The concepts are straightforward. The catch is in the details.

## Offset — it's just a record number

Each partition is an ordered log. Records in that log are numbered sequentially: 0, 1, 2, 3, and so on. That number is the offset.

A few things worth noting upfront:

- An offset is a position in a **specific partition**. Offsets are independent across partitions. Partition-0 at offset=42 and partition-1 at offset=42 are completely unrelated records.
- The broker assigns the offset on write — not the client. The producer writes, and the broker responds: "received, you got partition=2 offset=17". That pair (partition, offset) identifies the message uniquely and permanently.
- Offsets grow monotonically. A record with offset=10 always came before the record with offset=11. This is the basic ordering guarantee within a partition.

When a partition has messages, it always has two boundaries — earliest and latest. Earliest is the offset of the oldest live message. Latest is the offset that **the next write will receive** (one more than the offset of the most recent message already stored). On an empty topic, earliest=latest=0.

When a producer writes to a topic, latest grows. When retention sweeps old segments, earliest grows. The log "flows" — filled from the top, draining from the bottom.

## Log end offset, HWM, and leader epoch — what these terms mean

This is where confusion starts. Worth sorting out once and for all.

`Log End Offset (LEO)` — the position where the partition **leader** will write the next message. The offset of the "next record" that doesn't exist yet. The leader and each follower have their own LEO; the follower's LEO usually lags slightly because it pulls data asynchronously.

`High Watermark (HWM)` — the offset up to which a consumer is allowed to **read**. HWM = the minimum LEO across all replicas in the ISR. The idea is simple: until a message has been caught up by all ISR replicas, no one should see it — otherwise, after a failover, the new leader might not know about something a consumer already read. That would give us readable history that vanished after the switch. Kafka cannot allow that.

Between the leader's LEO and the HWM is a gap — records "committed by the leader but not yet replicated to ISR." Those records are already in the log, but invisible to the consumer.

`Leader Epoch` — a separate story. A counter that increments on every leader change. It's needed to correctly truncate follower logs after a switch. The course lectures don't touch it directly; knowing it exists and fixes rare failover bugs is enough.

In code, `kadm.ListEndOffsets` returns an offset equivalent to the HWM (for an in-sync client, that's the leader's LEO bounded by the ISR — Kafka doesn't expose uncommitted records).

```
partition: lecture-01-04-offsets-0

  earliest                                       latest = HWM
     │                                              │
     ▼                                              ▼
   ┌──────────────────────────────────────────────┐
   │ msg msg msg msg msg msg msg msg msg msg msg  │
   └──────────────────────────────────────────────┘
   offset:  17  18  19  20  21  22  23  24  25  26  27 ◄── next write lands here

   retained = latest - earliest = 27 - 17 = 10

   old segments (offsets 0..16) already deleted by retention
```

## Retention — two axes on which the log ages

The parameter that answers "how long do messages live." There are two.

`retention.ms` — by time. A log segment whose last record is older than `retention.ms` milliseconds is considered stale and deleted entirely. Default is 7 days (`604800000`).

`retention.bytes` — by size. When the total size of a partition on disk exceeds `retention.bytes`, the oldest segments are deleted until the size is back within bounds. Default is `-1`, meaning no size limit.

The parameters are **not mutually exclusive**. A segment is deleted if it hits **either** limit. You can set retention.ms=7 days + retention.bytes=10 GB — the segment goes when it ages out or when the partition grows to 10 GB, whichever comes first. This is common in production: time for delivery guarantees to consumers, size to prevent a traffic spike from filling the disk.

Here's the most common beginner mistake: "our retention.ms=86400000, so messages live exactly one day." No. They live **at least** one day and **at most** one day plus the duration of the active segment. The active segment (the one currently being written to) is never deleted. Retention looks at **the timestamp of the last record in a segment**, not individual messages. A message that landed at the very start of a segment's life will survive for retention.ms + segment.ms — until the segment closes, ages, and gets swept.

One more thing. Cleanup is deferred. The broker runs the retention checker once every `log.retention.check.interval.ms` (default 5 minutes). On the sandbox this is the default value — so in our demo `earliest` will jump in discrete steps every few minutes rather than moving smoothly.

## `__consumer_offsets` — where consumer positions live

We said brokers assign offsets. But who remembers where a consumer stopped reading? The consumer itself — and also Kafka, if the consumer opted in.

Inside the sandbox there's a system topic `__consumer_offsets`. 50 partitions (`offsets.topic.num.partitions=50` by default), replication factor matches `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` (3 in our setup). A consumer group writes its commit there: "group `lecture-01-06-group`, topic `lecture-01-05-first-producer`, partition=0 — committed offset=42."

A record in `__consumer_offsets` is a regular Kafka message. It's a compacted topic (cleanup.policy=compact, not delete) — it always holds the latest version of each key `(group, topic, partition)`. Old overwritten versions are removed by compaction; current ones are never removed. So positions survive broker restarts and are not lost to retention.

Important: **the offset in `__consumer_offsets` is a pointer to the next record to read.** "Consumer committed offset=42" means: "I processed records 0..41, start from 42 next time." That's exactly why on the first run of a consumer group Kafka sets the pointer to latest or earliest (based on `auto.offset.reset`) — there's no committed offset yet, so it needs to be initialized.

The [First consumer on franz-go](../../../01-06-first-consumer/i18n/en/README.md) lecture will open this topic through the consumer. Here we fix the model: consumer position is stored separately from the data, in a system compacted topic — and that's by design.

## `cleanup.policy` — delete and compact

Since we touched on `__consumer_offsets`, two lines on this. A topic has a `cleanup.policy` parameter that controls **how** Kafka cleans up the log.

- `delete` — standard behavior. Old segments are deleted according to retention.ms / retention.bytes. This is for regular event topics.
- `compact` — log compaction. No whole segments are deleted here. **Old versions of each key** are removed — the most recent record with key `K` lives forever (or until the next record with the same key). This is for state topics: latest user profile, latest config, committed consumer group offset.
- `delete,compact` — hybrid. Segments are compacted by key, and anything older than retention is deleted entirely.

Compaction gets a thorough treatment in [Retention and compaction](../../../../08-operations/08-02-retention-and-compaction/i18n/en/README.md) — that's where it belongs. Here: know that there's more than one option, and `__consumer_offsets` uses `compact`, not `delete`.

## What the code does

`cmd/load-and-watch/main.go` follows several steps. It creates the topic `lecture-01-04-offsets` with `partitions=3`, `rf=3`, **`retention.ms=60000`**, **`segment.ms=10000`**. Idempotent: if the topic already exists, the config is updated via `IncrementalAlterConfigs`. Then it writes 100 messages via `ProduceSync` with keys `k-0..k-99` — because the key is set, the partitioner places them deterministically across partitions (covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)).

Topic configs are passed directly to `CreateTopic` as the fourth argument — a map of `name → *string`:

```go
configs := map[string]*string{
    "retention.ms":   kadm.StringPtr(strconv.FormatInt(o.retention.Milliseconds(), 10)),
    "segment.ms":     kadm.StringPtr(strconv.FormatInt(o.segment.Milliseconds(), 10)),
    "cleanup.policy": kadm.StringPtr("delete"),
}

resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, configs, o.topic)
```

If the topic already exists, we fall through to `AlterTopicConfigs` with `SetConfig` operations to bring the existing config in line. This way a repeated run doesn't crash or get stuck with stale retention values.

After writing, a 10-second ticker starts. On each tick:

1. Writes one heartbeat message `hb-N`. Why — explained below.
2. Calls `kadm.ListStartOffsets` (earliest = log start offset).
3. Calls `kadm.ListEndOffsets` (latest = HWM).
4. Prints a table: PARTITION / EARLIEST / LATEST / RETAINED, plus TOTAL.

In code, two back-to-back requests — both return a map of `(topic, partition) → offset`:

```go
starts, err := admin.ListStartOffsets(rpcCtx, topic) // earliest = log start
ends,   err := admin.ListEndOffsets(rpcCtx, topic)   // latest   = HWM

starts.Each(func(o kadm.ListedOffset) {
    rows = append(rows, row{partition: o.Partition, earliest: o.Offset})
})
for i := range rows {
    if eo, ok := ends.Lookup(topic, rows[i].partition); ok && eo.Err == nil {
        rows[i].latest = eo.Offset
    }
}
// retained := latest - earliest — how many messages are live right now
```

The heartbeat write itself is a plain `ProduceSync` with key `hb-N`:

```go
rec := &kgo.Record{
    Topic: topic,
    Key:   []byte(fmt.Sprintf("hb-%d", n)),
    Value: []byte(fmt.Sprintf("heartbeat-%d", n)),
}
return cl.ProduceSync(rpcCtx, rec).FirstErr()
```

The heartbeat is needed because **a segment closes based on `segment.ms` from the last write into it**, and the active segment is never deleted. Without heartbeats, after the initial 100 messages the active segment lives forever — retention removes nothing, because the entire log sits in one unclosed segment. A heartbeat every 10 seconds rolls the current segment: it closes per `segment.ms`, a new one opens in its place — and the closed one can now be picked up by retention.

What you'll see when you run it:

```
[16:42:11]  heartbeats=0
PARTITION  EARLIEST  LATEST  RETAINED
0          0         34      34
1          0         33      33
2          0         33      33
TOTAL      0         100     100
---
```

Start. All 100 messages present. EARLIEST is 0 everywhere.

```
[16:43:21]  heartbeats=7
PARTITION  EARLIEST  LATEST  RETAINED
0          0         36      36
1          0         35      35
2          0         36      36
TOTAL      0         107     107
---
```

After a minute or two, LATEST grew (heartbeats added), EARLIEST still 0. Old segments exist, but the retention checker hasn't run yet.

After a few minutes (5–7, on the sandbox with the default `log.retention.check.interval.ms=300000`):

```
[16:48:31]  heartbeats=37
PARTITION  EARLIEST  LATEST  RETAINED
0          34        66      32
1          33        65      32
2          33        66      33
TOTAL      100       197     97
---
```

Here's the interesting part. EARLIEST on each partition jumped from 0 to 33–34. The retention checker ran, found segments whose max timestamp was older than 60s, and deleted them entirely. The original 100 records went with them — they're no longer readable by anyone. RETAINED shows "how many messages are currently in the log" — about 32 per partition (the recent heartbeats).

Leave the program running and the picture keeps drifting right. EARLIEST chases LATEST with a lag of `retention.ms + segment.ms + log.retention.check.interval.ms` — roughly 6–7 minutes.

## Running

The sandbox must be up (`docker compose up -d` from the root).

```sh
make run
```

In a second terminal, useful to compare against the CLI in parallel:

```sh
make topic-describe
```

You get `kafka-topics.sh --describe` (RF, partitions, leader/replicas/ISR — familiar from [Topics and partitions](../../../01-02-topics-and-partitions/i18n/en/README.md) and [Replication and ISR](../../../01-03-replication-and-isr/i18n/en/README.md)) plus `kafka-configs.sh --describe`, which shows the configured `retention.ms=60000`, `segment.ms=10000`, `cleanup.policy=delete`.

Restart from scratch:

```sh
make run RECREATE=true
```

To test retention more aggressively — set retention=10s, segment=5s:

```sh
make run RETENTION=10s SEGMENT=5s
```

Clean up after the lecture:

```sh
make topic-delete
```

## Why any of this matters

Three practical takeaways:

1. **"Stored for X days" means up to X days plus the segment duration.** If the contract with consumers requires "guaranteed last 7 days available" — set retention.ms to 7 days with margin, not exactly. And remember the active segment stays open until it closes.
2. **Earliest grows on its own.** A consumer that has fallen behind by more than the retention period will get `OFFSET_OUT_OF_RANGE` when trying to read its position — it simply doesn't exist in the log anymore. This is expected behavior, not a Kafka error. Configurable via `auto.offset.reset` (latest/earliest/none) — covered in [Offset commits](../../../../03-consumer/03-02-offset-commits/i18n/en/README.md).
3. **Retention.bytes is your friend.** Without it, one misbehaving producer with oversized messages can fill a broker's disk overnight. On critical topics, always set both limits — time and size.

Next up — [First producer on franz-go](../../../01-05-first-producer/i18n/en/README.md) — the first producer. Write 10 messages and see how the offset returned by `ProduceSync` fits directly into the model we just covered.
