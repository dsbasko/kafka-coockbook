# Kafka on Go — From Novice to Pro

> Available as an interactive site: https://dsbasko.github.io/kafka-cookbook/

A nine-module course with 42 units (38 lectures plus 4 cross-cutting use cases). Each lecture builds on the sandbox stack at the repo root — kafka-1/2/3 in KRaft mode, Schema Registry, Kafka Connect, Kafka UI.

The goal: go from "what is a topic and a partition" to production-grade patterns — transactions, outbox, CDC via Debezium, a gRPC + Kafka hybrid. No sugar. Real code that runs on your machine via `make run` and leaves an observable trace on the stack.

## Site

The same material as a static site with program navigation, syntax highlighting, language toggle (RU / EN), theme switch, and reading preferences (font size and family — separately for prose and code):

**https://dsbasko.github.io/kafka-cookbook/**

Local run (requires Node ≥ 20 and pnpm 9.15.0 — `corepack enable && corepack prepare pnpm@9.15.0 --activate`):

```sh
make web-install   # once — pnpm install in web/
make web-dev       # http://localhost:3000
make web-build     # static output in web/out/ (the same build runs in GitHub Actions on push to main)
```

Deployment is automatic: `.github/workflows/deploy.yml` builds and publishes on push to `main` (enable Settings → Pages → Source: "GitHub Actions" once). Origin for canonical / sitemap / OG comes from `NEXT_PUBLIC_SITE_URL` (default `https://dsbasko.github.io`).

## Stack

- Go 1.26
- Apache Kafka 4.2.0 (KRaft mode, no ZooKeeper)
- [twmb/franz-go](https://github.com/twmb/franz-go) v1.21.0 — Kafka client
- Protobuf and gRPC (codegen via buf)
- Confluent Schema Registry plus Kafka Connect (with Debezium for CDC)
- Postgres, ClickHouse, Elasticsearch, and mock services — for the module 09 use cases

Everything that runs in the stack and how to bring it up — see [docker-compose.yml](docker-compose.yml).

## Getting Started

First bring up the stack from the repo root (`docker compose up -d`). Then from the root:

```sh
make list                                                    # tree of lectures (lectures/<module>/<slug>)
make lecture L=01-foundations/01-01-architecture-and-kraft  # run a specific one
```

The root `Makefile` delegates these targets to `lectures/Makefile` and owns `connect-install-plugins` / `connect-verify-plugins` (because `connect-plugins/` lives in the root).

Every lecture is a separate Go module with its own `go.mod`, a README in both Russian and English (`i18n/ru/README.md`, `i18n/en/README.md` when translated), and a Makefile with `run` and `topic-create` targets plus something topic-specific (`kill-broker`, `proto-gen`, `connector-create` — depends on the lecture).

Client bootstrap is an environment variable, the default already fits the local stack:

```sh
KAFKA_BOOTSTRAP=localhost:19092,localhost:19093,localhost:19094
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_CONNECT_URL=http://localhost:8083
```

Logging — via `LOG_LEVEL=debug|info|warn|error` (default info, text to stderr).

## Structure

```
lectures/
├── go.work                     # workspace that ties all lecture modules together
├── README.md                   # dev-doc for adding new lectures
├── Makefile                    # delegates make into individual lectures
├── internal/                   # shared helpers — kafka client, env, runctx, log
│   ├── kafka/
│   ├── config/
│   ├── runctx/
│   └── log/
├── 01-foundations/             # module 01
│   └── 01-01-architecture-and-kraft/
│       ├── go.mod
│       ├── README.md           # stub linking to i18n/ru and i18n/en
│       ├── i18n/
│       │   ├── ru/README.md
│       │   └── en/README.md
│       ├── images/             # diagrams referenced from both READMEs
│       ├── Makefile
│       └── cmd/<binary>/main.go
├── 02-producer/                # module 02
├── ...
└── 09-use-cases/               # cross-cutting use cases (separate README, integration test)
```

Inside a lecture there are usually one or two binaries in `cmd/`. Use cases are bigger — several services, proto schemas, sometimes a `docker-compose.override.yml` for an extra Postgres / ClickHouse / Elasticsearch.

## Shared helpers

To avoid copying the same boilerplate into every lecture, common pieces live in `internal/`:

- `internal/kafka.NewClient(opts ...kgo.Opt)` — kgo.Client with course defaults (SeedBrokers from env, ClientID `lectures`, reasonable timeouts)
- `internal/kafka.NewAdmin()` — admin client for CreateTopic / DescribeTopics / Lag
- `internal/config.MustEnv(name)`, `EnvOr(name, default)` — wrappers over os.Getenv
- `internal/runctx.New()` — context cancelled on SIGINT / SIGTERM
- `internal/log.New()` — slog logger with level from LOG_LEVEL

Use it like this (pseudocode from a future lecture):

```go
ctx, cancel := runctx.New()
defer cancel()

cl, err := kafkactl.NewClient()
if err != nil { panic(err) }
defer cl.Close()
```

Each lecture lists in its `go.mod`:

```
require (
    github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0
    github.com/twmb/franz-go v1.21.0
)
```

The workspace resolves the local path to `internal` through `go.work`.

## Table of Contents

The course goes by difficulty. Modules 01–04 are the mandatory base; after that you can jump in any order (but 05 is needed before 09-03 / 09-04, and 07-04 builds on 04-03).

### 01 — Foundations

The Kafka base model: broker, controller, topic, partition, replication, offset, retention. The first producer and consumer on franz-go — after this module you have a working mental model that every other topic builds on.

- [01-01 — Architecture and KRaft](lectures/01-foundations/01-01-architecture-and-kraft/i18n/en/README.md)
- [01-02 — Topics and partitions](lectures/01-foundations/01-02-topics-and-partitions/i18n/ru/README.md) *(RU only)*
- [01-03 — Replication and ISR](lectures/01-foundations/01-03-replication-and-isr/i18n/ru/README.md) *(RU only)*
- [01-04 — Offsets and retention](lectures/01-foundations/01-04-offsets-and-retention/i18n/ru/README.md) *(RU only)*
- [01-05 — First producer on franz-go](lectures/01-foundations/01-05-first-producer/i18n/ru/README.md) *(RU only)*
- [01-06 — First consumer on franz-go](lectures/01-foundations/01-06-first-consumer/i18n/ru/README.md) *(RU only)*

### 02 — Producer

Where the one-key-one-partition guarantee lives, how acks affects durability, why you need idempotency, how batching and compression shift throughput, and which error classes the producer sees on retries.

- [02-01 — Keys and partitioning](lectures/02-producer/02-01-keys-and-partitioning/i18n/ru/README.md) *(RU only)*
- [02-02 — Acks and durability](lectures/02-producer/02-02-acks-and-durability/i18n/ru/README.md) *(RU only)*
- [02-03 — Idempotent producer](lectures/02-producer/02-03-idempotent-producer/i18n/ru/README.md) *(RU only)*
- [02-04 — Batching and throughput](lectures/02-producer/02-04-batching-and-throughput/i18n/ru/README.md) *(RU only)*
- [02-05 — Errors, retries, and headers](lectures/02-producer/02-05-errors-retries-headers/i18n/ru/README.md) *(RU only)*

### 03 — Consumer

Consumer groups and rebalances (including cooperative-sticky), offset commits, processing guarantees on the consumer side, error handling via retry topics and DLQ, concurrency and lag.

- [03-01 — Groups and rebalances](lectures/03-consumer/03-01-groups-and-rebalance/i18n/ru/README.md) *(RU only)*
- [03-02 — Offset commits](lectures/03-consumer/03-02-offset-commits/i18n/ru/README.md) *(RU only)*
- [03-03 — Processing guarantees](lectures/03-consumer/03-03-processing-guarantees/i18n/ru/README.md) *(RU only)*
- [03-04 — Error handling](lectures/03-consumer/03-04-error-handling/i18n/ru/README.md) *(RU only)*
- [03-05 — Concurrency and lag](lectures/03-consumer/03-05-concurrency-and-lag/i18n/ru/README.md) *(RU only)*

### 04 — Reliability

Transactions and exactly-once semantics, the consume-process-produce pattern, transactional outbox, retry/DLQ deep dive, delivery to external systems (an HTTP courier with circuit breaker and backpressure via PauseFetchPartitions).

- [04-01 — Transactions and EOS](lectures/04-reliability/04-01-transactions-and-eos/i18n/ru/README.md) *(RU only)*
- [04-02 — Consume-process-produce](lectures/04-reliability/04-02-consume-process-produce/i18n/ru/README.md) *(RU only)*
- [04-03 — Outbox pattern](lectures/04-reliability/04-03-outbox-pattern/i18n/ru/README.md) *(RU only)*
- [04-04 — Retry and DLQ deep dive](lectures/04-reliability/04-04-retry-and-dlq/i18n/ru/README.md) *(RU only)*
- [04-05 — External system delivery](lectures/04-reliability/04-05-external-delivery/i18n/ru/README.md) *(RU only)*

### 05 — Contracts

Why schemas matter and a comparison of wire formats (JSON / Avro / Protobuf), Protobuf in Go via buf, Schema Registry with magic byte and schema_id, schema evolution (BACKWARD / FORWARD / FULL), and what counts as a breaking change in protobuf.

- [05-01 — Why contracts and wire formats](lectures/05-contracts/05-01-why-contracts-and-wire-formats/i18n/ru/README.md) *(RU only)*
- [05-02 — Protobuf in Go](lectures/05-contracts/05-02-protobuf-in-go/i18n/ru/README.md) *(RU only)*
- [05-03 — Schema Registry](lectures/05-contracts/05-03-schema-registry/i18n/ru/README.md) *(RU only)*
- [05-04 — Schema evolution](lectures/05-contracts/05-04-schema-evolution/i18n/ru/README.md) *(RU only)*

### 06 — Communication patterns

For those who design systems: gRPC basics and streaming, sync vs async (a decision matrix), a gRPC + Kafka hybrid in one service, saga (choreography vs orchestration).

- [06-01 — gRPC: basics](lectures/06-communication-patterns/06-01-grpc-basics/i18n/ru/README.md) *(RU only)*
- [06-02 — gRPC streaming](lectures/06-communication-patterns/06-02-grpc-streaming/i18n/ru/README.md) *(RU only)*
- [06-03 — Sync vs async: gRPC and Kafka](lectures/06-communication-patterns/06-03-sync-vs-async/i18n/ru/README.md) *(RU only)*
- [06-04 — gRPC + Kafka hybrid](lectures/06-communication-patterns/06-04-hybrid-grpc-and-kafka/i18n/ru/README.md) *(RU only)*
- [06-05 — Saga: choreography vs orchestration](lectures/06-communication-patterns/06-05-saga-choreography/i18n/ru/README.md) *(RU only)*

### 07 — Streams and Connect

Stream processing concepts (event-time, windowing, watermark, late events), a word-count implementation on franz-go + Pebble (a Kafka Streams replacement for Go), Kafka Connect, and Debezium CDC.

- [07-01 — Stream processing: concepts](lectures/07-streams-and-connect/07-01-stream-processing-concepts/i18n/ru/README.md) *(RU only)*
- [07-02 — Stream processing in Go (franz-go + Pebble)](lectures/07-streams-and-connect/07-02-stream-processing-in-go/i18n/ru/README.md) *(RU only)*
- [07-03 — Kafka Connect](lectures/07-streams-and-connect/07-03-kafka-connect/i18n/ru/README.md) *(RU only)*
- [07-04 — Debezium CDC](lectures/07-streams-and-connect/07-04-debezium-cdc/i18n/ru/README.md) *(RU only)*

### 08 — Operations

Monitoring and metrics (kminion + Grafana), retention vs compaction in practice, topic sizing and tuning for a load profile, a troubleshooting runbook for common problems.

- [08-01 — Monitoring and metrics](lectures/08-operations/08-01-monitoring-and-metrics/i18n/ru/README.md) *(RU only)*
- [08-02 — Retention and compaction](lectures/08-operations/08-02-retention-and-compaction/i18n/ru/README.md) *(RU only)*
- [08-03 — Sizing and tuning](lectures/08-operations/08-03-sizing-and-tuning/i18n/ru/README.md) *(RU only)*
- [08-04 — Troubleshooting runbook](lectures/08-operations/08-04-troubleshooting-runbook/i18n/ru/README.md) *(RU only)*

### 09 — Use cases

End-to-end scenarios that tie everything above into working applications with integration tests. Use cases are larger than lectures: several services, proto schemas, sometimes a docker-compose.override.yml for Postgres / ClickHouse / Elasticsearch.

- [09-01 — Microservices communication](lectures/09-use-cases/01-microservices-comm/i18n/ru/README.md) *(RU only)* — 3 services × 2 nodes, gRPC + outbox + Kafka
- [09-02 — Push notifications](lectures/09-use-cases/02-push-notifications/i18n/ru/README.md) *(RU only)* — router → Firebase / APNs / webhook with retry + DLQ, circuit breaker, HMAC, replay, integration test
- [09-03 — Postgres → ClickHouse with anonymization](lectures/09-use-cases/03-pg-to-clickhouse/i18n/ru/README.md) *(RU only)* — Debezium + Go anonymizer with declarative YAML + ClickHouse Sink, ReplacingMergeTree(cdc_lsn) for dedup, integration test
- [09-04 — Postgres → Elasticsearch](lectures/09-use-cases/04-pg-to-elasticsearch/i18n/ru/README.md) *(RU only)* — Debezium + Confluent ES Sink via ExtractNewRecordState SMT, index template, blue-green reindex via alias rotation, integration test

## How to add a new lecture

Inside a module, create a folder named `<MM>-<short-name>/`, drop a `go.mod` with module path `github.com/dsbasko/kafka-sandbox/lectures/<NN-module>/<MM-short>`, add `use ./<NN-module>/<MM-short>` to `lectures/go.work`, then `i18n/ru/README.md` + Makefile + cmd. Declare the lecture in `course.yaml` (id, title in both `ru` and `en`, duration, tags) and run `make web-check-coverage` — it reconciles the manifest with the filesystem and prints any mismatches. Add the entry to the TOC section above by running `make web-generate-readme-toc` and pasting the output.

## Compatibility and out of scope

The course is a learning sandbox. Production configs (security, ACL, mTLS, multi-DC replication, MirrorMaker2) are not covered — they require real infrastructure and step outside the local-machine setup. Alternative clients (Sarama, confluent-kafka-go) are not covered either: the course intentionally sticks to one — franz-go.

If something on the stack breaks — check `docker-compose.yml` and container logs (`docker compose logs kafka-1`).
