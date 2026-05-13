# 08-04 — Troubleshooting Runbook

Eight course modules done — time to consolidate everything into one note you come back to at 3 AM when Slack alerts are piling up. This lecture is a runbook. A list of typical incidents, worked top to bottom: symptom → diagnosis → action. No philosophy. A practical checklist.

The idea is simple. When something is on fire, there's no time to read long docs. You want a quick table: "see X — check Y — turn Z." Below are twelve such blocks, plus three small programs that show a few problems from the client's perspective.

## How to read this runbook

Each entry has three paragraphs. Symptom (exactly what you see — an alert, a metric, user behavior). Diagnosis (where to get the facts: kafka-ui, kadm, JMX, broker logs). Action (what to adjust and in what order). If the action requires inspecting your own cluster, the block links to a program from this lecture or to the course lecture where it was covered.

Nothing new appears here. This is a collection of what was already covered in modules 02–08. Just in "saw it — did it" format.

## 1. Under-replicated partitions

Symptom. The metric `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` rose from zero. The cluster dashboard shows a red "UR=N" counter. Alert: "replica out of sync".

Diagnosis. First — check how many brokers are in metadata. If three out of three — one broker is alive on the network but the fetcher can't keep up. If two out of three — it's clear who's missing (`docker ps`, JMX consumer, our `under-replicated-watch`). Next — `kafka-topics.sh --describe --under-replicated-partitions` or `ListTopics` via kadm to identify the affected topics. If UR has been there for minutes and isn't clearing — dig into the follower broker logs: usually `OutOfMemoryError`, disk at 100%, or GC pauses.

Action. Broker is down — bring it up (`docker start kafka-2` on our sandbox). Broker is alive but not catching up — check `replica.fetch.max.bytes`, disk, network. If UR < ISR_min with `acks=all` — producers will start getting `NOT_ENOUGH_REPLICAS`, that's already a write incident, see block 4.

## 2. High consumer lag

Symptom. Lag on the group grows linearly or spikes. Business complains "new events aren't appearing in the UI" or "state is 10 minutes stale". Alert: "consumer lag > N".

Diagnosis. First — steady growth or plateau? Growing linearly — the producer writes faster than the consumer can process. Plateau at one level — the consumer is dead, making no new progress. Check `kadm.Lag(group)` or `kafka-consumer-groups.sh --describe`. Get lag per-partition. If skewed — one partition grows, the rest are zero — that's a hot partition, see block 8. If all are growing evenly — load exceeded worker throughput. If one consumer left and partitions have no owner — rebalance didn't finish, see block 3.

Action. Too few workers — add instances (or more partitions — but that's planning, not a runbook). One processing thread is heavy — check whether you can parallelize per-key via the worker pool from the [Concurrency and lag](../../../../03-consumer/03-05-concurrency-and-lag/i18n/en/README.md) lecture. External system (DB, HTTP) is slow — check it, apply backpressure via `cl.PauseFetchPartitions` (see [Delivery to external systems](../../../../04-reliability/04-05-external-delivery/i18n/en/README.md)), don't silently accumulate in-flight.

## 3. Frequent rebalances

Symptom. Consumer logs are full of `Revoking ... Assigning`. The metric `kafka.consumer.coordinator.rebalance-rate` is non-zero. Lag jumps in spikes — after each rebalance, part of the caches is evicted and state is repartitioned.

Diagnosis. What's triggering the rebalance? Three common causes. (1) A worker can't `poll` within `max.poll.interval.ms` — the coordinator considers it dead. (2) `session.timeout.ms` is too short, a GC pause is longer — same result. (3) Deployments constantly spin instances up and down, and rebalances are a side effect of normal scaling.

Action. (1) — increase `max.poll.interval.ms` or speed up processing (same `cl.PauseFetchPartitions`, splitting heavy work across workers). (2) — increase `session.timeout.ms`, investigate GC. (3) — switch to `cooperative-sticky` (`kgo.Balancers(kgo.CooperativeStickyBalancer())`), so only reassigned partitions move during rebalance, not all of them. All of this was covered in [Groups and rebalancing](../../../../03-consumer/03-01-groups-and-rebalance/i18n/en/README.md) — this is just a reminder.

## 4. Producer error rate ↑

Symptom. The producer dashboard shows a rising error count. Logs contain `NotEnoughReplicas`, `RequestTimedOut`, `RecordTooLargeException`, `UnknownTopicOrPartition`, `InvalidProducerEpoch`. Business: "my orders are disappearing".

Diagnosis. Classify by error type.
- `NotEnoughReplicas` — ISR dropped below `min.insync.replicas`. Go to block 1.
- `RequestTimedOut` — the broker didn't respond within `request.timeout.ms`. Broker is overloaded or the network degraded.
- `RecordTooLargeException` — the client is sending more than `max.message.bytes`. Not retriable, retrying won't help. Inspect the payload, consider offloading blobs to external storage.
- `UnknownTopicOrPartition` — the topic was deleted, or auto-create isn't configured and the producer is writing to a nonexistent topic. Create it idempotently via kadm.
- `InvalidProducerEpoch` — someone else started with the same `transactional.id`. This is zombie fencing, see [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md).

Action. First classify retriable/non-retriable. Retriable — franz-go retries automatically up to `RetryTimeout`, usually just wait it out. Non-retriable — fix the code or config, retries are useless. And the main rule: DO NOT suppress errors in the callback. A producer error means the write status is unknown. Silently dropping it means losing data.

## 5. Disk growing

Symptom. The broker's `du` level is climbing. Alert: "kafka data dir > 80%".

Diagnosis. What's accumulating? Three options.
- A topic with `retention.ms=-1` (compact or simply infinite). Segment sizes are normal for CDC state, but still need monitoring (see profiles in [Sizing and tuning](../../../08-03-sizing-and-tuning/i18n/en/README.md)).
- Retention is configured but not triggering — `segment.ms` is too large, the active segment doesn't close, retention doesn't touch it (see [Retention and compaction](../../../08-02-retention-and-compaction/i18n/en/README.md)). Seeing 70% of disk under one segment — this is usually the cause.
- Tombstones aren't being cleaned up — `min.cleanable.dirty.ratio` is high, the log cleaner isn't working much. The tail remains.

`kafka-log-dirs.sh --describe` on kafka-1 will give a breakdown by topic. Then `kafka-configs.sh --entity-type topics --describe` on the problem topic and compare with what you expect.

Action. Topic is normal but sending too much — reduce retention or increase disk. Segment is huge — cut `segment.ms`/`segment.bytes`, wait for rotation. Compaction is lagging — lower `min.cleanable.dirty.ratio` to 0.1. If it's a real emergency — manually deleting old partitions requires stopping the broker, and that's the last resort.

## 6. Controller bouncing

Symptom. In the KRaft cluster the controller node keeps changing. The metric `kafka.controller:type=KafkaController,name=ActiveControllerCount` is fluctuating. Topic creation/deletion is hanging.

Diagnosis. Controller node logs — usually visible there: either quorum loss (`__cluster_metadata` can't gather a majority), or GC on one of the controllers is kicking it out of the quorum. On our sandbox combined-mode — broker and controller on the same JVM, so if the broker is under load, the controller suffers too. In production, separate the roles.

Action. If the quorum isn't converging — verify all controller nodes are alive and can see each other on the network (port 9093 on our sandbox). If one node is slow due to GC — heap, JVM flags, restart as a last resort. Without an active controller, DDL operations (CreateTopic, DescribeConfigs alter) hang.

## 7. Broker won't start

Symptom. After a restart the broker doesn't come up. Logs contain `RuntimeException`, `Failed to recover`, `Inconsistent log directory`.

Diagnosis. Most common — a corrupted segment after a hard `kill -9` or OOM. The broker log usually names the file it couldn't open. Second — node.id conflict: after `docker compose down -v` volumes were recreated, but meta.properties in the data directory is left over from an old installation (if volumes weren't wiped). Third — port taken by another process.

Action. Corrupted segment — `LogManager` tries to recover on startup; if that fails — move the file aside, let the broker start, replicas will replicate from others. meta.properties conflict — clear it. Port — `lsof -i :9092` and resolve the conflict. On our sandbox, `docker compose down && up` fixes 90% of problems (including cases where logs were compiled incorrectly).

## 8. Hot partition

Symptom. Lag grows on only one partition, the rest are zero. Throughput on the topic has hit the ceiling of a single worker, and adding workers doesn't help (new ones sit idle — all partitions are already assigned).

Diagnosis. Which partition is the concentration on? `ListEndOffsets` before and after a short load window gives you `delta` per-partition. If 80% of writes go to one — that's a hot key. If the skew is smaller (20–30% difference) — normal murmur2 noise on small volumes, don't panic.

This picture is exactly what `cmd/hot-partition-demo` from this lecture produces. One key `hot` writes at 1000/sec, ten regular user keys at 10/sec each. After 10 seconds you can see the partition where `murmur2('hot')` landed receiving 85+% of total volume.

Action. Composite key — `cmd/composite-key-fix`. Take the former hot key and append a suffix `:bucket-N`, where `N = hash(payload_id) % buckets`. Logically it's still "a hot stream of one type", but physically it's spread across `buckets` partitions. After the change — the skew disappears. The cost — you lose the `one-key-one-partition` guarantee for the hot key, and if per-key ordering matters, you need to preserve grouping by sub-key within the bucket. If ordering doesn't matter — composite key solves hot partition for free.

## 9. Partition reassignment stuck

Symptom. You ran `kafka-reassign-partitions.sh --execute`, and `--verify` hangs on `... still in progress`. For hours. UR partitions won't clear.

Diagnosis. What's replicating slowly? `kafka-replica-verification.sh` or comparing fetch metrics. Most often the problem is `replication.throttled.replicas` set too low — replication is throttled to 10 MB/sec but you need to move terabytes.

Action. Raise `replica.alter.log.dirs.io.max.bytes.per.second` (or the broker equivalent). Check for background tasks eating disk (compaction, large segment rotation). If the move is progressing normally but the topic is huge — it just takes time; the JSON plan has a progress metric.

## 10. Topic deletion stuck

Symptom. `kafka-topics.sh --delete --topic foo` completed, but the topic shows up in `--list` with the `_marked_for_deletion` tag. Files on disk aren't being removed.

Diagnosis. The broker has `delete.topic.enable` disabled (it defaults to `true` on newer versions, but check). Or — the broker holding that replica is down, and until it comes back, the controller can't finish the delete.

Action. Verify `delete.topic.enable=true` on all brokers. Bring the failed node back up. If completely stuck (old bug, very rare) — restart the controller node. Barely encountered on recent Kafka versions.

## 11. Schema Registry rejects

Symptom. Producer writes, sends a request to SR to register a new schema version. SR responds 409 Conflict with body `Schema being registered is incompatible with an earlier schema for subject "X"`.

Diagnosis. Run `buf breaking --against` locally (if Protobuf). See exactly what broke: removed field, changed type, didn't reserve the tag. If the subject compatibility is `BACKWARD` — you can't remove required fields. If `FORWARD` — you can't add required ones. If `FULL` — neither. See [Schema evolution](../../../../05-contracts/05-04-schema-evolution/i18n/en/README.md).

Action. Roll back the schema change. Fix the `proto` file — add the new field as `optional` (or with a default), don't touch existing tags, don't change types. Re-release. If you urgently need to roll out the old version right now — change the subject compatibility only deliberately (knowing that other consumers may start throwing `Unmarshal` errors).

## 12. Connector failed

Symptom. On kafka-connect, `/connectors/<name>/status` via REST returns `state: FAILED`. The trace has an exception. Source or sink isn't writing.

Diagnosis. Most common causes. (1) Credentials — Postgres password was changed, Debezium slot won't open. (2) Plugin not found — kafka-connect started without the required class in `plugin.path`, see section 34.5 on installation. (3) Source isn't receiving changes — Postgres slot "crashed" or WAL is filling up. (4) Sink isn't writing to downstream — ClickHouse/ES is unavailable, trace shows HTTP 5xx.

`docker logs kafka-connect | tail -200` usually gives the full history. Then fix point by point.

Action. (1) and (3) — fix on the Postgres side. (2) — reinstall the plugin (see 34.5). (4) — fix downstream and `restart` the connector via REST. If the connector is "stuck in FAILED" — `pause` → `resume`, sometimes `delete` + `create` (if data isn't critical).

## What the programs demonstrate

This lecture has three short binaries.

### hot-partition-demo

Creates a topic with four partitions, RF=3. Runs two generators in parallel — one with key `hot` at 1000 messages/sec, the second with ten user keys at 10/sec each. Output — a distribution table across partitions with shares and a bar. You can see that 80+% of total volume went to one partition and the rest are idle.

The write loop itself uses `cl.Produce` with a callback. Rate is controlled by `time.Ticker`:

```go
tick := time.Second / time.Duration(rate)
t := time.NewTicker(tick)
for {
    select {
    case <-ctx.Done():
        return sent
    case <-t.C:
        for _, k := range keys {
            rec := &kgo.Record{
                Topic: topic,
                Key:   []byte(k),
                Value: []byte("event"),
            }
            cl.Produce(ctx, rec, func(_ *kgo.Record, err error) { ... })
            sent++
        }
    }
}
```

Measurements — via `kadm.ListEndOffsets` before and after the window. The difference is the delta per-partition:

```go
ends, err := admin.ListEndOffsets(rpcCtx, topic)
ends.Each(func(o kadm.ListedOffset) {
    if o.Err != nil { return }
    out[o.Partition] = o.Offset
})
```

This is more reliable than counting in memory on the producer side: what matters is "what actually sits in the partitions", not "what was sent". If we only counted `sent`, the skew wouldn't be visible because the producer callbacks might not have fired yet.

### composite-key-fix

Same scenario, but instead of one `hot` it writes four composite keys: `hot:bucket-0`, `hot:bucket-1`, `hot:bucket-2`, `hot:bucket-3`. `murmur2` distributes them across partitions and the stream spreads out. The total `hot-rate` is divided by `buckets` so the comparison with the previous binary is fair:

```go
hotPerKey := o.hotRate / o.buckets
if hotPerKey < 1 { hotPerKey = 1 }
hotKeys := make([]string, o.buckets)
for i := 0; i < o.buckets; i++ {
    hotKeys[i] = fmt.Sprintf("hot:bucket-%d", i)
}
```

In real code the bucket index is derived not by sequential counter but via `hash(payload_id) % buckets` — so the same logical object always lands in the same bucket. Per-object ordering is preserved, and the skew is gone. In our demo we just cycle through all buckets in order — that's enough for illustration.

### under-replicated-watch

A cluster dashboard in one loop. Every `interval` it calls `ListBrokers` and `ListTopics`, counts under-replicated partitions, prints a summary and a table of problem partitions.

The core is a simple check via `len`:

```go
for _, t := range td {
    if t.Err != nil { continue }
    for _, p := range t.Partitions {
        if len(p.ISR) < len(p.Replicas) {
            urParts++
        }
    }
}
```

This is the same formula as the JMX metric `UnderReplicatedPartitions` on the broker. We're just observing from the client's perspective, without connecting to JMX. Works as long as we can reach at least one broker — `ListTopics` routes itself to the metadata leader via franz-go.

The lecture scenario — run `make run-watch` in one terminal, `make kill-broker` in another. On the next tick you can see the broker is gone from `BROKERS` and partitions where it was in `Replicas` have gone UR. After `make restore-broker` — back to green.

## Running

```sh
make help                 # cheat sheet
make run-hot              # hot-partition-demo, see the single-partition skew
make run-fixed            # composite-key-fix, see the evening out
make run-watch            # cluster dashboard, updates every 3s
make run-watch-once       # single tick and exit (for tests)
make kill-broker          # docker stop kafka-2 — trigger UR
make restore-broker       # docker start kafka-2 — bring back online
```

Parameters:

```sh
HOT_RATE=2000 NORMAL_RATE=20 DURATION=20s make run-hot
BUCKETS=8 make run-fixed                                 # more buckets — better balancing
WATCH_INTERVAL=1s make run-watch                          # faster ticks (loads the Connect API on the sandbox)
BROKER=kafka-3 make kill-broker                           # take down a different node
```

Also watch kafka-ui (http://localhost:8080) during `kill-broker` — the main screen also shows the UR counter, and each topic's partitions page highlights "out of sync" in color. Some incidents from the runbook are easier to catch there than on the command line.

## Cheat sheet

| Symptom | First command | Where to go when it's really on fire |
|---------|--------------|--------------------------------------|
| UR partitions ↑ | `make run-watch-once` | block 1 |
| Lag growing | `kafka-consumer-groups.sh --describe` | blocks 2, 3, 8 |
| Frequent rebalance | `grep -i revoking` in logs | block 3 |
| Producer errors ↑ | classify by error message | block 4 |
| Disk ↑ | `kafka-log-dirs.sh --describe` | block 5 |
| Controller bouncing | `ActiveControllerCount` in JMX | block 6 |
| Broker won't start | `docker logs kafka-N` | block 7 |
| Hot partition | `make run-hot` (see the balance?) | block 8 |
| Reassignment stuck | `kafka-reassign-partitions.sh --verify` | block 9 |
| Delete stuck | `--list` look for `_marked_` | block 10 |
| SR rejects | `buf breaking --against` locally | block 11 |
| Connector failed | `docker logs kafka-connect` | block 12 |

This isn't the full list of what can break. It's the baseline set of "what you'll encounter in the first month of cluster life". The longer you live with Kafka, the longer your own runbook grows. This one is the starting point.
