# 01-02 — Topics and partitions

In the previous lesson we established that Kafka is a distributed append-only log. The word "distributed" is exactly what this lesson is about. Topic and partition — two terms you cannot move forward without.

## What is a topic

A topic is a named channel that producers write to and consumers read from. At the model level it's straightforward: name the topic `orders` — orders go there; name it `payments` — payments. No exchanges or routing keys, unlike RabbitMQ. Messages simply live under that name.

From the storage perspective, a topic is a directory on the broker's disk. Inside the directory — log files split into segments. When a segment reaches the `segment.bytes` or `segment.ms` limit, it closes and a new one opens. Old segments are dropped by retention (see [Offsets and retention](../../../01-04-offsets-and-retention/i18n/ru/README.md)). That's everything at the physical level. No magic.

But if a topic were a single file, everything would bottleneck at one node. One disk, one CPU, one network card, one broker process. Easy to hit the ceiling. So topics are split into partitions.

## Partition — the unit of parallelism

A partition is a shard of a topic. An independent append-only log. Topic `orders` with three partitions is three independent logs: `orders-0`, `orders-1`, `orders-2`. Each partition lives on its own set of brokers (covered in [Replication and ISR](../../../01-03-replication-and-isr/i18n/ru/README.md)), and each has its own leader through which writes flow.

Why this model? All the other properties of Kafka follow from it:

- writes parallelize — producers can write to different partitions through different leaders simultaneously;
- reads parallelize — multiple consumers in the same group divide partitions among themselves (one consumer per partition maximum);
- storage scales horizontally — more partitions = more even distribution across brokers;
- ordering is guaranteed only within a partition; across partitions it doesn't exist in general.

The last point is the main stumbling block for newcomers. Clients want "order all my events for me", and Kafka honestly replies: I'll order within a partition. Across partitions, ordering is undefined. If you need all events for a given `userID` to arrive in the right sequence, put the userID in the key and they will all land in one partition (see below).

## How a message lands in a partition

When a producer writes, it sends `(topic, key, value)`. The client then decides which partition to assign it to:

- if `key` is empty — round-robin or sticky partitioner (the default in franz-go is sticky: it accumulates a batch and sends it to one partition for efficiency);
- if `key` is present — `partition = hash(key) mod N`, where N is the number of partitions.

The default hash is murmur2 (same as the Java client, so Go and Java write to the same partition for the same key). You can set a custom one via `kgo.Partitioner`. This is covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/ru/README.md); for now the key fact is: key → partition via simple arithmetic.

This formula leads to a property that interview questions love. If you write messages with the same key, they all land in the same partition. Guaranteed. And therefore they are read in order. That is the "one-key, one-partition, one-order" guarantee.

```
                       topic = orders, partitions = 3
                       hash(key) mod 3

  key="user-1"  ──┐                          ┌──> partition-0
  key="user-4"  ──┤   (h(k)%3 == 0)          │    [r0 r1 r2 ...]
  key="user-7"  ──┘                          │
                                             │
  key="user-2"  ──┐                          ├──> partition-1
  key="user-5"  ──┤   (h(k)%3 == 1)          │    [r0 r1 r2 ...]
  key="user-8"  ──┘                          │
                                             │
  key="user-3"  ──┐                          └──> partition-2
  key="user-6"  ──┤   (h(k)%3 == 2)               [r0 r1 r2 ...]
  key="user-9"  ──┘
```

Each partition is an ordered sequence of records with a monotonically increasing offset (`r0`, `r1`, `r2`, …). Offsets across partitions are not comparable — each has its own counter starting from zero.

## Why you cannot reduce the number of partitions

This is where every other person trips up. Partitions can only be **added** to a topic. You cannot reduce the count. Not with any admin request. Not with any `alter`.

The reason is the `hash(key) mod N` formula. You created a topic with N=3, wrote messages for a year. Every key landed in its partition. If tomorrow you set N=2, then for the same key `hash(key) mod 2` yields a completely different number. The data for the same key ends up "historically in one partition, new data in another". The entire per-key ordering guarantee collapses. Kafka simply doesn't offer the operation — there's no button to press by mistake.

Expansion also breaks the distribution, by the way: at N=3 → N=4, the same keys that landed in partition 0 can now land in any of the four. So `kafka-topics --alter --partitions` is typically run on a fresh topic, or when a temporary key-ordering disruption is acceptable. On production topics this is not a one-click operation.

Practical takeaway. Plan the partition count upfront, with room to grow. Not "10 partitions because it's a round number", but "N — because expected throughput / per-partition limit". Rough estimate: a partition handles ~10–20 MB/s of writes and the same for reads. If you expect 100 MB/s, you want at least 6–8 partitions, better with headroom to 12–16. This is empirical — detailed breakdown in [Sizing and tuning](../../../../08-operations/08-03-sizing-and-tuning/i18n/ru/README.md).

## Topic naming convention for the course

All course topics follow the naming scheme `lecture-<MM>-<NN>-<short>`. This lesson's topic is `lecture-01-02-orders`. This makes ownership clear and cleanup easy — `kafka-topics --list | grep ^lecture-`.

Use cases (module 09) use different names — `orders`, `notification-events`, `notifications-retry-30s`. No prefix, because those scenarios already resemble production.

## What the program demonstrates

`cmd/inspect/main.go` does three things:

1. **Creates the topic** idempotently. It first tries via `admin.CreateTopic(ctx, partitions, rf, configs, topic)`. If it hits `kerr.TopicAlreadyExists`, that's treated as normal: the topic already exists, so describe it instead. So running `make run` a second time succeeds quietly — it just prints the current state.
2. **Describes the topic** via `admin.ListTopics(ctx, "lecture-01-02-orders")`. Under the hood this is a metadata request to the broker. Returns `TopicDetails` — a map of name → details: `TopicID`, the internal flag, partition count, per-partition leader/replicas/ISR.
3. **Prints a per-partition table**. Shows who the leader is, which nodes hold replicas, and which of them are in ISR.

The `-recreate=true` flag first deletes the topic, then creates it again. Useful for seeing how the controller distributes leaders across nodes — the Kafka balancer typically spreads leadership evenly (3 partitions on 3 nodes → one leader per node).

Here is the core of idempotent creation. `CreateTopic` returned `TopicAlreadyExists` — that's not a failure, that's "already there":

```go
func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) (bool, error) {
    resp, err := admin.CreateTopic(ctx, partitions, rf, nil, topic)
    if err == nil && resp.Err == nil {
        return true, nil
    }

    cause := err
    if cause == nil {
        cause = resp.Err
    }
    if errors.Is(cause, kerr.TopicAlreadyExists) {
        return false, nil
    }
    return false, cause
}
```

After `ensureTopic`, `ListTopics` is called — that's the metadata request:

```go
details, err := admin.ListTopics(rpcCtx, topic)
td := details[topic]
// td.Topic       — имя
// td.ID          — TopicID (UUID)
// td.Partitions  — мапа partition → детали (Leader, Replicas, ISR, OfflineReplicas)
```

And the table print itself. `Partitions.Sorted()` returns a slice sorted by partition number:

```go
parts := td.Partitions.Sorted()
for _, p := range parts {
    offline := fmt.Sprintf("%v", p.OfflineReplicas)
    if len(p.OfflineReplicas) == 0 {
        offline = "-"
    }
    fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n",
        p.Partition, p.Leader, p.Replicas, p.ISR, offline)
}
```

What's in the table is what's in `PartitionDetail`. `LEADER` is `p.Leader`, `REPLICAS` is `p.Replicas`, `ISR` is `p.ISR`, `OFFLINE` is `p.OfflineReplicas`. The print just inserts numbers into the format string.

## Running it

The sandbox must be running (`docker compose up -d` from the repo root).

```sh
make run
```

Expected output (IDs and leaders will differ on your machine):

```
topic "lecture-01-02-orders" создан: partitions=3 rf=3

Topic:       lecture-01-02-orders
TopicID:     kcFo++q0QQ+xaKj0pnwWGA==
Partitions:  3

PARTITION  LEADER  REPLICAS  ISR      OFFLINE
0          2       [2 3 1]   [2 3 1]  -
1          3       [3 1 2]   [3 1 2]  -
2          1       [1 2 3]   [1 2 3]  -
```

Things to note:

- `LEADER` differs per partition — write load is spread across nodes.
- `REPLICAS` — three numbers (RF=3), and the order in the list is the preferred leader: the first entry is who the controller wants as leader. The controller tries, but it doesn't always succeed immediately — leader election picks a live replica, not the "right" one.
- `ISR == REPLICAS` — all replicas are in sync. That means at `acks=all` writes are acknowledged immediately. If a node went down, ISR would shrink (covered in [Replication and ISR](../../../01-03-replication-and-isr/i18n/ru/README.md)).
- `OFFLINE` is empty — all replicas are alive.

Compare with the CLI:

```sh
make topic-describe
```

This target runs `kafka-topics.sh --describe --topic lecture-01-02-orders` inside the kafka-1 container. The picture is the same (field names differ slightly, but `Leader/Replicas/Isr` match). That's the lesson's point — what the distribution does with a shell script, franz-go does in one line: `admin.ListTopics`.

Want to see how leader assignment changes?

```sh
make topic-recreate
```

Deletes the topic and recreates it. On the freed partitions, the controller picks leaders by preferred-replica logic. Run it a few times — you'll notice the numbers are stable (the controller doesn't choose randomly), but on delete and recreate the partition assignments across nodes differ.

After the lesson, clean up:

```sh
make topic-delete
```

## What you learned

- A topic is a named channel; on disk it's a directory of log file segments.
- A partition is an independent append-only log within a topic; the unit of parallelism for reads and writes.
- A message with a non-empty key always lands in the same partition via `hash(key) mod N`.
- Kafka guarantees ordering only within a partition; across partitions it is undefined.
- Partitions can be added but not removed — otherwise the `hash(key) mod N` mapping breaks retroactively.
- `admin.ListTopics` returns metadata: per-partition leader, replicas, ISR — enough to understand the current topic state without a shell.

Next ([Replication and ISR](../../../01-03-replication-and-isr/i18n/ru/README.md)) we dig into what `Replicas` and `ISR` actually mean — what "a replica fell behind" means, how ISR shrinks when a node goes down, and where `min.insync.replicas` fits in.
