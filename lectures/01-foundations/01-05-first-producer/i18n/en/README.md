# 01-05 — First Producer

Up to this lecture we looked at Kafka from the broker side. What's inside a topic, how it replicates, where the offset lives, how the log ages. Now we switch sides. This is the producer — a program that writes to a topic.

The goal is modest: write 10 messages and see how the `(partition, offset)` pair maps to the model we covered earlier. No `acks=all`, no idempotency, no batching, no compression — all that is in module 02. This is raw, basic writing.

## What kgo.Client is

In franz-go, `kgo.Client` is a **long-lived object**. One per process, one per service. Not "open-write-close" on every message the way you would with an HTTP request. The client holds an internal connection pool to the brokers, a topic metadata cache, and background goroutines for batching and delivery. Creating a client is expensive; writing through an existing client is cheap.

Basic initialization:

```go
cl, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:19092", "localhost:19093", "localhost:19094"),
    kgo.ClientID("first-producer"),
)
if err != nil { ... }
defer cl.Close()
```

`SeedBrokers` are **entry points**, not "the full broker list". The client connects to any of the listed addresses, fetches the current list of all cluster nodes through it, and works with that from then on. Listing any 1–3 addresses is enough; in the course we list all three in case one is down at startup.

In our code the client is created through `internal.kafka.NewClient` — a wrapper with course-level defaults. SeedBrokers are taken from `KAFKA_BOOTSTRAP`, ClientID is `lectures`, plus sensible dial and retry timeouts. For non-default options, pass them as the second argument — they are appended last and can override the defaults.

## kgo.Record — what we actually write

`kgo.Record` is a struct that describes a single message. Not JSON, not proto, not a string — just a description of bytes.

```go
rec := &kgo.Record{
    Topic:     "lecture-01-05-first-producer",
    Key:       []byte("k-7"),
    Value:     []byte("hello-7"),
    Headers:   []kgo.RecordHeader{{Key: "type", Value: []byte("greeting")}},
    Partition: -1, // -1 = let the partitioner decide; an explicit partition number also works
}
```

Key points:

- `Key` and `Value` are `[]byte`. Serialization (JSON/Protobuf/Avro) is your responsibility. Kafka does not parse the data.
- `Topic` is required. `Partition` usually is not — the partitioner picks it based on the key (covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md)).
- `Headers` are `[]byte` pairs. Use them for metadata that is convenient to read without deserializing the payload: `trace_id`, `message_type`, `source_service`. We cover them in detail in [Errors, retries and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/en/README.md); here we just note they exist.
- `Timestamp` can be set manually or left zero; in that case the broker substitutes its own time at the moment of writing. The final timestamp in the log is also affected by `message.timestamp.type` at the topic level (`CreateTime` vs `LogAppendTime`), but that's an operational concern.

After a successful write the broker returns the populated `Partition`, `Offset`, and `Timestamp` — the ones in the log. That is the "address" of our message.

## ProduceSync vs Produce

franz-go has two ways to write.

`Produce` is asynchronous. It places the record in the client's internal buffer, **returns immediately**, and notifies delivery via a callback later:

```go
cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
    if err != nil { /* log the error */ return }
    fmt.Printf("partition=%d offset=%d\n", r.Partition, r.Offset)
})
```

You can fire a million calls in milliseconds; the client batches and sends them on its own. This is the "fast path". It suits high-throughput producers where per-record latency does not matter, only aggregate throughput does. It returns control immediately; errors arrive later through the callback and must be handled carefully (covered in [Batching and throughput](../../../../02-producer/02-04-batching-and-throughput/i18n/en/README.md) and [Errors, retries and headers](../../../../02-producer/02-05-errors-retries-headers/i18n/en/README.md)).

`ProduceSync` is synchronous. It blocks until the record receives an acknowledgement from the broker and returns `kgo.ProduceResults`:

```go
res := cl.ProduceSync(ctx, rec)
if err := res.FirstErr(); err != nil { ... }
fmt.Printf("partition=%d offset=%d\n", res[0].Record.Partition, res[0].Record.Offset)
```

This is the "slow but straightforward path". Control returns only after the broker responds. You can pass multiple records in a single call — `ProduceSync(ctx, rec1, rec2, rec3)` — and get back a slice of results in the same order. For a teaching lecture where you need to see partition+offset immediately, `ProduceSync` is ideal. In production on hot paths, `Produce` is the usual choice because the synchronous variant blocks a goroutine per write.

There is no difference in guarantees under the hood. Both use the same session, the same batching (if multiple records fit into one batch before sending), the same retry logic. `ProduceSync` is essentially `Produce` plus a `WaitGroup` plus error aggregation.

## What guarantees we have right now

Honestly: **minimal**.

franz-go enables idempotency by default (this was not always true; it is now). That means our 10 records will not be duplicated on a retry after a network loss. The broker sees the producer ID + sequence number and drops the duplicate.

We did not set `acks` explicitly — and the franz-go default for production writes is `acks=all`. That means the leader waits for acknowledgement from all ISR replicas before responding to the producer. Given that our sandbox has RF=3 and `min.insync.replicas=2`, our write is "safe": it is on disk on at least two nodes by the time we receive the response.

What we do NOT have here:

- Transactions. If the process crashes between writes, partial records remain in the log as ordinary messages. There is no atomic "either all 10 or none" group. That is the topic of [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md).
- Protection against zombie producers. If this process hangs, someone forks it, and both continue writing — both will write. Idempotency does not fix that; it only works within a single producer session. That is also covered in [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/en/README.md).
- A single producer instance across multiple topics with consistency guarantees. One client writing to ten topics writes to each independently.

What we do have: writes are guaranteed to reach 2 ISR replicas, without duplicates within the session, in the order of `ProduceSync` calls. For most everyday producers, that is enough.

## What our code does

`cmd/producer/main.go` does exactly what was promised.

1. Creates a `kgo.Client` via the shared helper.
2. Creates the topic `lecture-01-05-first-producer` idempotently: `partitions=3`, `rf=3`. If it already exists, it moves on silently.
3. Loops from 0 to 9, builds a `kgo.Record` with `Key="k-N"`, `Value="hello-N"`, and writes via `ProduceSync`.
4. After each write, prints a table row — N, KEY, VALUE, PARTITION, OFFSET, BROKER-TS.
5. After the loop, calls `kadm.ListEndOffsets`, prints the per-partition latest and the total. On a freshly created topic the total equals exactly 10 — clear proof that the records landed.

The write loop itself is the "bare" producer work of the course:

```go
for i := 0; i < o.messages; i++ {
    key := fmt.Sprintf("k-%d", i)
    val := fmt.Sprintf("hello-%d", i)
    rec := &kgo.Record{
        Topic: o.topic,
        Key:   []byte(key),
        Value: []byte(val),
    }

    rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
    res := cl.ProduceSync(rpcCtx, rec)
    rpcCancel()
    if err := res.FirstErr(); err != nil {
        return fmt.Errorf("produce %d: %w", i, err)
    }
    got := res[0].Record
    fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%s\n",
        i, key, val, got.Partition, got.Offset,
        got.Timestamp.Format("15:04:05.000"))
}
```

`got.Partition` and `got.Offset` are **what the broker returned**, not what we requested. Those are the coordinates in the log. The partition came from the key via `hash(key) mod N`; the offset was issued by the partition leader at write time.

After the loop — a final check via `ListEndOffsets`:

```go
ends, err := admin.ListEndOffsets(rpcCtx, topic)
ends.Each(func(o kadm.ListedOffset) {
    fmt.Fprintf(tw, "%d\t%d\n", o.Partition, o.Offset)
    total += o.Offset
})
fmt.Fprintf(tw, "TOTAL\t%d\n", total)
```

The sum of latest offsets across all partitions of a fresh topic equals the number of written messages. Run the program a second time and the sum becomes 20, and so on.

What you will see in the output:

```
topic "lecture-01-05-first-producer" created: partitions=3 rf=3

writing 10 messages to topic "lecture-01-05-first-producer" via ProduceSync

N  KEY  VALUE     PARTITION  OFFSET  BROKER-TS
0  k-0  hello-0   0          0       16:55:01.234
1  k-1  hello-1   2          0       16:55:01.241
2  k-2  hello-2   1          0       16:55:01.247
3  k-3  hello-3   0          1       16:55:01.253
4  k-4  hello-4   2          1       16:55:01.259
5  k-5  hello-5   1          1       16:55:01.265
6  k-6  hello-6   0          2       16:55:01.271
7  k-7  hello-7   2          2       16:55:01.277
8  k-8  hello-8   1          2       16:55:01.283
9  k-9  hello-9   0          3       16:55:01.289

done. Now the same picture from the log side:
PARTITION  LATEST
0          4
1          3
2          3
TOTAL      10
```

A few observations from this output.

Each record got **its own** offset within its partition. Records in partition 0 have offsets 0, 1, 2, 3 — four messages, latest=4. Partition 1 has three messages, latest=3. The sum of latest across all partitions is 10. Everything adds up.

Partition assignment is **deterministic**, not random. The same key always lands in the same partition. Restart the program with the same set of keys and the distribution repeats (but offsets advance because it is a new write on top of the existing log). The partitioner logic is covered in [Keys and partitioning](../../../../02-producer/02-01-keys-and-partitioning/i18n/en/README.md).

The specific offset values and distribution in your output may differ. If you ran the program twice without `topic-delete`, all offsets will be 10 higher in total because the second run appended on top of the first. That is expected.

## Running

The sandbox must be up (`docker compose up -d` from the repo root).

```sh
make run
```

In a second terminal you can watch the same topic via the CLI consumer in parallel:

```sh
make consume-cli
```

This is `kafka-console-consumer.sh` running inside `kafka-1` with the `--from-beginning` flag. It prints partition, offset, key, and value. You should see the same 10 messages our program produced — Kafka does not distinguish a "message from a Go client" from a "message from kafka-console-producer"; they look identical in the log.

Describe the topic via `kafka-topics.sh`:

```sh
make topic-describe
```

Clean up after the lecture:

```sh
make topic-delete
```

## Why any of this matters

This is the foundation everything in module 02 builds on. After this lecture the mental model should be:

1. **kgo.Client is long-lived.** Create it once, reuse it.
2. **kgo.Record is raw bytes plus addressing (topic + key + headers).** You own serialization.
3. **ProduceSync — for learning and infrequent writes. Produce + callback — for hot paths.** The delivery guarantees are identical.
4. **The broker issues the offset, not the client.** The returned `(partition, offset)` pair is the message's coordinate in the log for its entire lifetime.

Next up — [First consumer on franz-go](../../../01-06-first-consumer/i18n/en/README.md). We will read those 10 messages back, confirm the offsets in the output match what `ProduceSync` returned, and get a first look at what a committed offset of a consumer group actually means via `auto-commit`.
