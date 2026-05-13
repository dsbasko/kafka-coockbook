# 05-01 — Why Contracts & Wire Formats

In the first four modules of this course we wrote text to Kafka. Sometimes `hello-world`, sometimes JSON, sometimes made-up `payments` with little attention to shape. It worked because all producers and consumers were in our hands, and we kept the message shape agreement in our heads. That stops working now.

The moment a second service appears — written by someone else, or a third one in a different language, or a fourth one two years later — your "agreement" becomes the thing that breaks the integration. Someone renamed a field. One side expected an `int`, got a `string`. Someone simply forgot the field existed.

This lecture is about how to avoid that. Specifically — about the tool used to avoid it: a message contract, plus a wire format and schema as the way to lock it down in code. On to business.

## Why JSON breaks at scale

JSON is, at its core, text with curly braces. Serialize an object, get a string, put it in Kafka. Human-readable, supported in every language, a pleasure to debug. Up to a point, it's the best thing available.

That point arrives fast. Fields in JSON are just names. If two services both say "order_amount" and one day someone writes "orderAmount" on their side — nothing crashes. The other side just gets `null` or a zero value, and you notice it N hours into production after broken orders have piled up.

JSON makes no distinction between "field absent" and "field explicitly null". No distinction between the number 42 and the string "42". No concept that this field is a timestamp rather than just int64. No validation — you can send `{"id": [1,2,3]}` instead of `{"id": "ord-001"}` at any time, and Kafka will accept it without complaint, because to the broker it's all equally bytes.

Size. JSON stores field names in every message. The field `customer_id` in a million messages means `customer_id` a million times. Topic-level compression helps, but the raw payload size grows linearly with the number of fields and the length of their names.

## What a schema is

A schema is a contract about message shape, defined separately from the message itself. Somewhere you have a file that says: "Order is a struct with fields id (string), customer_id (string), amount_cents (int64), currency (string), items list". All producers must send exactly that shape. All consumers rely on it. When someone changes the schema — that is a visible change: through a PR, through review, through an automated compatibility check and (often) through a registry migration.

Next comes the format itself. Text or binary. Field names inside the message or in a separate schema. How numbers, strings, arrays, nested objects, and optional fields are encoded. That is the wire format.

In the Kafka ecosystem, four options are most often compared: plain JSON (no schema — an antipattern at scale, but common), JSON Schema (same JSON, but with a separate schema and validation), Avro, and Protobuf. The course picks Protobuf — why, below.

## Seven comparison criteria

When choosing a format, compare on the things that matter. Here are the reference points.

**Payload size.** How many bytes one record takes on the wire. JSON loses here; Avro and Protobuf are compact. In our benchmark (see below) Avro turns out slightly more compact than Protobuf on short fields — because Avro writes no field names in the payload at all, only values in schema order. Protobuf writes a field tag (a number) before each non-empty value.

**Encoding and decoding speed.** With modern Go libraries the differences are within reason — JSON is slightly slower, Avro and Protobuf faster. Measure against your own profile — numeric fields go at one speed, strings and nested structures at another. At high volume the data flow will hit the network/disk before it hits the CPU anyway.

**Contract.** JSON has no contract by default. Avro requires a schema — without it the data cannot be decoded. Protobuf is the same: a `.proto` file and generated code are required. With Avro and Protobuf you cannot "accidentally" break the contract, because data and schema live together.

**Compatibility.** What happens if the producer upgrades before the consumer. JSON without compatibility rules → luck of the draw. JSON Schema provides rules, but discipline is on the developer. Avro and Protobuf have formal BACKWARD/FORWARD/FULL compatibility concepts that are checked automatically (Schema Registry in the Avro world, `buf breaking` in Protobuf). Details in [Schema evolution](../../../05-04-schema-evolution/i18n/en/README.md).

**Evolution.** Can you add a field, remove one, or rename one without a break. Avro and Protobuf have formal rules for this. A new field must get a new field number. A removed number must never be reused. The required→optional transition and its reverse are checked separately. JSON formally allows anything, but in practice you're just deferring the pain.

**Interop.** How many languages are supported. JSON — everywhere. Avro — major languages plus the JVM stack primarily. Protobuf — everywhere, because Google created it and every major infrastructure project drags it along (gRPC, Envoy, Kubernetes, etcd). If you have a polyglot stack, this is a serious argument.

**Tooling and ecosystem.** Schema Registry works for all three, but Avro was its first-class citizen (it's a Confluent tool, grew alongside Avro). Protobuf is newer here, but IDE tooling, code generation, and linters are stronger on the Protobuf side. gRPC is Protobuf, which is a huge advantage if you already have gRPC between services (module 06).

## Why the course picks Protobuf

A decision is made. The course could have taken Avro — also a good choice, especially if Schema Registry is already running and analytics are on the JVM. But we chose Protobuf for three reasons.

1. **Interop with gRPC.** Module 06 is entirely about communication. First gRPC, then a gRPC + Kafka hybrid. If you're already writing `.proto` files for service contracts, it makes sense to reuse the same types for events. One `.proto` file — two channels: synchronous (gRPC) and asynchronous (Kafka). No need to maintain two parallel universes.
2. **Tooling.** `buf` (which we'll cover in [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md)) is a modern, ergonomic tool: lint, breaking-change detection, formatting, clean workflow. Avro has fewer equivalents and they are heavier.
3. **IDE and codegen.** Generated Go code from Protobuf is normal typed structs that the Go toolchain sees as any ordinary package. Avro in Go works too (`hamba/avro` is an excellent library), but requires more manual schema work at runtime.

Avro beats Protobuf in one clear scenario: if your primary workload is analytics/data lake (Spark, Flink, Iceberg, Hive), Avro is native there. If your primary workload is inter-service communication — Protobuf.

## What our benchmark shows

See `cmd/format-bench/main.go`. The program does three things. First it generates the same set of Orders with a fixed seed — this is critical, otherwise the comparison is unfair. Then it serializes each Order three ways and writes to three different topics. Finally it asks Kafka for the on-disk sizes.

```go
orders := generateOrders(*count, *itemsPerOrder)

for i := range stats {
    t0 := time.Now()
    bytes, err := publishAll(ctx, cl, stats[i].topic, orders, encoders[i])
    if err != nil { ... }
    stats[i].bytesOnWire = bytes
    stats[i].duration = time.Since(t0)
}
```

`encoders` is a slice of three functions. JSON via `encoding/json`, Avro via `hamba/avro` with the schema from `avro/order.avsc`, Protobuf via `google.golang.org/protobuf/encoding/protowire` — encoded by hand, without code gen.

Why Protobuf by hand instead of `protoc-gen-go`? Because this lecture is about wire format as an idea. The goal is to show that there is no magic inside Protobuf bytes — just a simple format `tag (field_number << 3 | wire_type) + value`. The `encodeProto` function fits in 15 lines:

```go
func encodeProto(o *Order) ([]byte, error) {
    var buf []byte
    buf = appendString(buf, 1, o.ID)
    buf = appendString(buf, 2, o.CustomerID)
    buf = appendInt64(buf, 3, o.AmountCents)
    buf = appendString(buf, 4, o.Currency)
    buf = appendInt64(buf, 5, o.CreatedAtUnix)
    for i := range o.Items {
        item := encodeOrderItem(&o.Items[i])
        buf = protowire.AppendTag(buf, 6, protowire.BytesType)
        buf = protowire.AppendBytes(buf, item)
    }
    return buf, nil
}
```

The numbers `1, 2, 3, 4, 5, 6` are field numbers from `proto/order.proto`. There is no other connection to the `.proto` file here — we are doing by hand what codegen normally does. In [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md) we will replace all of this with generated `proto.Marshal`.

`appendString` is a wrapper for proto3 semantics where empty strings are not written on the wire:

```go
func appendString(buf []byte, fieldNum protowire.Number, v string) []byte {
    if v == "" {
        return buf
    }
    buf = protowire.AppendTag(buf, fieldNum, protowire.BytesType)
    return protowire.AppendString(buf, v)
}
```

This is proto3 default omission: if a field equals the zero value of its type, it is absent on the wire. The decoder substitutes the zero value on read. The saving — empty fields consume no bytes.

Avro works differently. The schema is external (in `.avsc`), and the payload is bare values in order:

```go
encoders[1] = func(o *Order) ([]byte, error) {
    return avro.Marshal(avroSchema, o)
}
```

If you have two dozen fields with short values, Avro will be more compact than Protobuf, because it writes no field tags. But if the schema is lost, Avro bytes are undecodable. Protobuf partially decodes even without the `.proto` file — the tags hint at types.

## Sizes

A run with defaults (`make run`, count=100000, items=3) on our sandbox gives roughly this picture:

```
=== payload bytes ===
format     payload     avg/rec
JSON       28_829_000   ~288 B
Avro        8_440_000    ~84 B
Protobuf   10_080_000   ~101 B
```

JSON is three times larger than Avro. Protobuf is slightly larger than Avro — due to field tags. On longer strings the gap narrows; on short numeric messages Avro wins by a wider margin. At `make run COUNT=1000000` the numbers multiply by 10 — linearly.

To see what is on disk:

```sh
make du-topics
```

This target reaches into `/var/lib/kafka/data` on each node via `docker exec` and counts partition sizes. Inside the sandbox RF=3, so total size across three nodes is ~3× the size of one replica.

## When to use which format

In practice nothing is cut and dry, but rough rules work.

JSON without a schema — only when the system is small, temporary, or you are building a prototype. No stack settled yet — use JSON, and if the system is still alive after six months, migrate to a schema. Waiting longer is painful.

JSON Schema — a compromise for teams where everything is already JSON and there are no resources for a full migration. Schema Registry works with JSON Schema; the main upside is payload validation against the schema on every write. The downside — it does not solve the size problem and catches schema evolution poorly.

Avro — if you have an analytics pipeline around the JVM, Spark, Iceberg. Schema Registry for Avro is the most mature. If your Kafka primarily feeds a data lake — Avro is a sound choice.

Protobuf — if you have inter-service communication over gRPC, a multi-language stack, and events in Kafka are just another communication channel between the same services. This is our case in the course.

## What to lock in

The wire format is a decision made once, and it drags behind it a long chain: from development tooling to how the system behaves during an incident where "the producer was updated, consumers were not". Do not carry JSON into production just because "it's obvious." It's obvious exactly once — at the start. After that everyone needs to understand what shape Order has today, in a year, and in five years.

Next in module 05: Protobuf in Go ([Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md)), Schema Registry with magic byte and schema_id ([Schema Registry](../../../05-03-schema-registry/i18n/en/README.md)), and schema evolution without breaking compatibility ([Schema evolution](../../../05-04-schema-evolution/i18n/en/README.md)). By the end of the module you will have a working setup where a `.proto` file change is verified by CI and registered in the Registry automatically.

## Running

```sh
# start the sandbox from the repo root if not already running
docker compose up -d

# from this directory
make run                       # default — 100k Orders, payload+disk table
make run COUNT=1000000         # one million — numbers become more pronounced
make du-topics                 # independent size check via du
make topic-delete-all          # clean up after yourself
```

`proto-gen` is intentionally a no-op in this lecture. In [Protobuf in Go](../../../05-02-protobuf-in-go/i18n/en/README.md) we will replace the manual wire encoding with generated protoc-gen-go code, and `proto-gen` will become a real target with buf.
