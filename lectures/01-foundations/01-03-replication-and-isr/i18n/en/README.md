# 01-03 — Replication and ISR

In the previous lecture we split a topic into partitions and saw that each partition has a `LEADER`, `REPLICAS`, and `ISR`. I waved it off and said "that's for 01-03". This is 01-03.

The topic splits in two. First, what replication is and why it exists. Then we run watch-isr and manually take down a broker — and watch ISR shrink and recover.

## Why replication exists

A partition is a file on the broker's disk. If the broker goes down, the file is gone. The messages stored in it can't be read. That's unconditionally bad.

The solution is obvious: keep copies. Each partition is stored on multiple brokers at once — that's replication. How many copies is set by `replication.factor` (RF) at the topic level. The sandbox defaults to RF=3 (`KAFKA_DEFAULT_REPLICATION_FACTOR=3`), and three nodes each hold one copy of every partition.

RF gives two guarantees:

- **Availability.** With RF=3, you can lose one or two brokers and keep reading and writing (depends on settings).
- **Durability.** A written message is already on multiple disks, not sitting in RAM on a single node.

Replication isn't free. Every message travels the network RF times and occupies RF×size on disk. On a prod cluster with serious traffic, that's real money. RF=3 is a sensible default. RF=5 is for highly critical topics. Higher is almost never necessary.

## Leader, follower, replica

Each partition has one `LEADER` and the rest are `followers`. They all sit on their brokers' disks the same way; the difference is only the role.

The leader does all the work:

1. Accepts writes from the producer (producers always write to the leader, never to a follower).
2. Appends to its local log.
3. Replicates to followers over the network.

Followers just replicate — they pull fresh records from the leader and save them locally. They don't serve producer requests and generally don't serve consumer requests (there is `Fetch From Follower` for multi-DC, but that's a separate story and irrelevant for a single data center).

The leader is elected by the controller (KRaft). If the leader goes down, the controller picks a new one from the live followers and the role transfers. Both the producer and the consumer learn about it — they re-elect the leader on the fly via metadata-refresh.

## ISR — which followers are "in sync"

Here it gets interesting. Followers pull data from the leader asynchronously. One follower might lag by hundreds of milliseconds; another might be down and pull nothing until it's fixed. Which ones count as live, and which as lagging?

That's what **ISR — In-Sync Replicas** is for. It's the subset of `REPLICAS` containing those that:

- fetched fresh data from the leader within `replica.lag.time.max.ms` (default 30 seconds);
- caught up to the leader's offset within that window.

If a follower stalls, falls behind on the network, or its `kafka` restarts — it drops out of ISR. This happens after `replica.lag.time.max.ms`: while the timer ticks the follower is considered live; after it expires, the follower is removed. Once it recovers and catches up to the end of the log, it rejoins ISR. This is normal: ISR is dynamic and constantly recalculated by the controller.

What matters: only replicas in ISR can become the new leader on failover (assuming `unclean.leader.election` is disabled — it is on the sandbox). That means no data loss: the new leader knows every message the old leader acknowledged. That's exactly why `acks=all` waits for ISR writes — replicas outside ISR don't count toward the quorum.

## min.insync.replicas — the write threshold

Without this parameter, RF is half a guarantee. It answers: **how many ISR replicas must acknowledge a write before a producer with `acks=all` gets OK**.

The sandbox has `KAFKA_MIN_INSYNC_REPLICAS=2`, which means:

- If ISR=3 — a write with `acks=all` is acknowledged normally; all good.
- If ISR=2 — also acknowledged; the cluster operates in a reduced state, but writes continue.
- If ISR=1 — a write with `acks=all` fails with `NotEnoughReplicas`. The producer retries (in case ISR recovers) and eventually gives up and returns an error. Reads still work.

The combination `RF=3 + min.insync.replicas=2 + acks=all` is the standard durable configuration. You can lose one broker and keep writing. Lose two — you can no longer write with durability guarantees (only without them, via `acks=1` or `acks=0`, but that means potential data loss).

`min.insync.replicas` is stored **on the topic** (or on the broker as a default). Different topics can have different values — a critical payments topic with min.insync=2, a telemetry topic with min.insync=1 so it keeps working even when two nodes are down.

## What it looks like

```
              partition: lecture-01-03-replicated-0
              RF=3, min.insync.replicas=2

   ┌─ kafka-1 (id=1) ─── replica  ─┐
   │                               │
   ├─ kafka-2 (id=2) ─── LEADER  ──┼── ISR={1,2,3}  ✓ acks=all OK
   │                               │
   └─ kafka-3 (id=3) ─── replica  ─┘


   stop kafka-2 → leader moves to kafka-3 (new leader)
   replica id=2 drops from ISR after ~30s

   ┌─ kafka-1 (id=1) ─── replica  ─┐
   │                               │
   ├─ kafka-2 (down)               │── ISR={1,3}    ✓ acks=all OK (2 of 3)
   │                               │
   └─ kafka-3 (id=3) ─── LEADER ───┘   under-replicated = yes


   start kafka-2 → catches up, rejoins ISR after ~5–30s

   ┌─ kafka-1 (id=1) ─── replica  ─┐
   │                               │
   ├─ kafka-2 (id=2) ─── replica  ─┼── ISR={1,2,3}  ✓ recovered
   │                               │
   └─ kafka-3 (id=3) ─── LEADER ───┘
```

Which replica specifically becomes the new leader depends on which replica in ISR was first in the `Replicas` list (preferred leader logic). Your exact numbers will differ — what matters is that the pattern is the same.

## The failure scenario we'll reproduce

Run `make run` — the program creates topic `lecture-01-03-replicated` with RF=3 and prints a table every 2 seconds:

```
[16:42:11]
PARTITION  LEADER  REPLICAS  ISR      UNDER-REPLICATED
0          2       [1 2 3]   [1 2 3]  no
1          3       [1 2 3]   [1 2 3]  no
2          1       [1 2 3]   [1 2 3]  no
---
```

All good. ISR is full, each partition has its own leader node, no under-replication.

In a separate terminal:

```sh
make kill-broker
```

That's `docker stop kafka-2`. After a few seconds watch-isr shows:

```
[16:42:21]
PARTITION  LEADER  REPLICAS  ISR    UNDER-REPLICATED
0          1       [1 2 3]   [1 3]  yes (missing [2])
1          3       [1 2 3]   [1 3]  yes (missing [2])
2          1       [1 2 3]   [1 3]  no   ← leader was already 1, replica id=2 still in ISR
---
```

After 30 seconds (`replica.lag.time.max.ms`), replica id=2 drops out of ISR for the last partition too:

```
[16:42:51]
PARTITION  LEADER  REPLICAS  ISR    UNDER-REPLICATED
0          1       [1 2 3]   [1 3]  yes (missing [2])
1          3       [1 2 3]   [1 3]  yes (missing [2])
2          1       [1 2 3]   [1 3]  yes (missing [2])
---
```

What happened: partition 0 had leader=2 — and 2 went down. The controller picked a new leader from ISR (id=1), writes continued without downtime. Partition 1 already had leader=3, nothing needed to change. Partition 2 lived on leader=1 — also no switch. Under the hood, leader elections and metadata-refreshes happened for all clients; our logs don't show that, but the `LEADER` column reflects the current truth.

Under-replication shows for all three partitions. The cluster is still operational because `min.insync.replicas=2` and ISR=2 — the threshold is met. But the safety margin is gone: one more node down and `acks=all` starts returning `NotEnoughReplicas`.

Restore the broker:

```sh
make restore-broker
```

After a few seconds watch-isr shows id=2 catching up to the leader and rejoining ISR. If nothing was written during the downtime — recovery is instant (nothing to catch up on). If there was traffic — proportional to the volume. After full recovery:

```
[16:43:25]
PARTITION  LEADER  REPLICAS  ISR      UNDER-REPLICATED
0          1       [1 2 3]   [1 2 3]  no
1          3       [1 2 3]   [1 2 3]  no
2          1       [1 2 3]   [1 2 3]  no
---
```

Note that leaders stayed where they ended up after failover. By default the controller does not move leaders back to their "historically correct" node; `auto.leader.rebalance.enable` and periodic leader rebalance handle that, but with a delay. The behavior is intentional — it avoids an unnecessary switch. On prod clusters, admins run `kafka-leader-election.sh --election-type preferred` manually or wait for auto-rebalancing.

## What the code does

`cmd/watch-isr/main.go` does three things. Creates the topic idempotently via `admin.CreateTopic` (if it already exists, uses it without recreating; in this lecture recreating interferes with observation). Starts a timer with the given `-interval`. On each tick, calls `admin.ListTopics(ctx, topic)` and prints `Partitions.Sorted()`.

The `UNDER-REPLICATED` column is `len(p.ISR) < len(p.Replicas)`. When `yes` — some replicas have dropped out; the `missing` function finds the specific IDs.

The observation loop is a plain ticker with a context check:

```go
t := time.NewTicker(interval)
defer t.Stop()

if err := tick(ctx, admin, topic); err != nil { ... }
for {
    select {
    case <-ctx.Done():
        return nil
    case <-t.C:
        if err := tick(ctx, admin, topic); err != nil {
            // Don't exit on a single metadata error: if a broker goes down,
            // the client will switch to a live one on its own. Just log and
            // continue — otherwise watch-isr loses its purpose during failover.
            fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
        }
    }
}
```

One tick is `ListTopics` plus printing. The `under-replicated` logic is literally a length comparison:

```go
for _, p := range td.Partitions.Sorted() {
    under := "no"
    if len(p.ISR) < len(p.Replicas) {
        under = fmt.Sprintf("yes (missing %v)", missing(p.Replicas, p.ISR))
    }
    fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n",
        p.Partition, p.Leader, p.Replicas, p.ISR, under)
}
```

The `missing` function finds replicas that are in `Replicas` but absent from `ISR` — those are the lagging nodes:

```go
func missing(replicas, isr []int32) []int32 {
    in := make(map[int32]struct{}, len(isr))
    for _, id := range isr {
        in[id] = struct{}{}
    }
    out := make([]int32, 0, len(replicas)-len(isr))
    for _, id := range replicas {
        if _, ok := in[id]; !ok {
            out = append(out, id)
        }
    }
    return out
}
```

One important code detail: a `ListTopics` error on a tick **does not kill the loop**. If the broker the client was connected to goes down, `franz-go` will pick a new seed broker on its own — but one or two requests in between may fail. If we exited on the first error, watch-isr would close exactly when the broker goes down (i.e., at the most interesting moment). So errors are logged and the loop continues.

## Running

The sandbox must be running (`docker compose up -d` from the root).

```sh
make run
```

In a separate terminal:

```sh
make kill-broker     # stop kafka-2
make restore-broker  # bring it back up
```

You can stop any other node:

```sh
make kill-broker BROKER=kafka-3
make restore-broker BROKER=kafka-3
```

Compare with the CLI:

```sh
make topic-describe
```

You get the same ISR as in watch-isr, just in shell-script format — `Leader: 1 Replicas: 1,2,3 Isr: 1,3`. The idea is exactly the same as in [Topics and partitions](../../../01-02-topics-and-partitions/i18n/ru/README.md): `admin.ListTopics` returns everything needed, no shell calls required.

Delete the topic:

```sh
make topic-delete
```

## When ISR is lost entirely

Suppose kafka-2 **and** kafka-3 go down simultaneously. One broker remains. A single node can't maintain durability with min.insync=2 — that's a hard stop.

```
ISR={1}     min.insync.replicas=2     →     write with acks=all → NotEnoughReplicas
```

What happens:

- A write with `acks=all` retries `kgo.RequestRetries` times and eventually fails with `NOT_ENOUGH_REPLICAS`. Downstream consumers will notice that someone stopped writing.
- A write with `acks=1` still works — but durability is gone if the last leader goes down.
- Reads work, the leader is alive. You can read everything committed before.

That's the point of `min.insync.replicas`. Kafka doesn't pretend everything is fine when it isn't. You declare a minimum replica count. Below it — stop, no writes; getting an error and an alert beats losing data on the next failure.

## What you learned

- Replication = copies of a partition on multiple brokers. RF is set at the topic level. The sandbox defaults to RF=3.
- Each partition has one leader; producers write only to the leader, followers pull from it.
- `ISR` — followers that are "in sync" (no more than `replica.lag.time.max.ms` behind). Only ISR replicas can be elected as the new leader on failover (with unclean.leader.election disabled).
- `min.insync.replicas` sets the threshold: how many ISR replicas must acknowledge a write with `acks=all`. On the sandbox it's 2.
- `RF=3 + min.insync.replicas=2 + acks=all` — the standard durable configuration. Survives one node failure; at ISR=1 `acks=all` starts returning `NotEnoughReplicas`.
- `admin.ListTopics` shows everything needed to observe ISR. No shell scripts required.

Next ([Offsets and retention](../../../01-04-offsets-and-retention/i18n/ru/README.md)) we look at how messages live in time. We'll cover offset, log end offset, HWM, and retention. Along the way, we'll understand why "our messages are stored for 7 days" is a phrase with a hidden catch.
