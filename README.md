# Курс: Kafka на Go от новичка до профессионала

Это учебный курс из 9 модулей и 41 единицы материала (37 лекций плюс 4 сквозных use case'а). Курс наслаивается поверх sandbox-стенда из корня репозитория — kafka-1/2/3 в KRaft mode, Schema Registry, Kafka Connect, Kafka UI.

Цель — пройти путь от «что такое топик и партиция» до production-grade паттернов: транзакции, outbox, CDC через Debezium, гибрид gRPC + Kafka. Без сахара. С реальным кодом, который запускается на твоей машине через `make run` и оставляет наблюдаемый эффект на стенде.

## Стек

- Go 1.26
- Apache Kafka 4.2.0 (KRaft mode, без ZooKeeper)
- [twmb/franz-go](https://github.com/twmb/franz-go) v1.21.0 — Kafka-клиент
- Protobuf и gRPC (кодогенерация через buf)
- Confluent Schema Registry плюс Kafka Connect (с Debezium для CDC)
- Postgres, ClickHouse, Elasticsearch и mock-сервисы — для use case'ов модуля 09

Полный список того, что крутится в стенде, и как его поднять — в [docker-compose.yml](docker-compose.yml).

## Как пользоваться

Сначала подними стенд из корня репозитория (`docker compose up -d`). Дальше из корня:

```sh
make list                                                    # дерево лекций (lectures/<module>/<slug>)
make lecture L=01-foundations/01-01-architecture-and-kraft  # запустить конкретную
```

Корневой `Makefile` делегирует эти цели в `lectures/Makefile`, плюс владеет `connect-install-plugins` / `connect-verify-plugins` (потому что `connect-plugins/` остаётся в корне).

Каждая лекция — отдельный Go-модуль со своим `go.mod`, README на русском и Makefile с целями `run`, `topic-create`, плюс что-нибудь специфичное (`kill-broker`, `proto-gen`, `connector-create` — зависит от темы).

Bootstrap для клиентов — переменная окружения, дефолт уже подходит для локального стенда:

```sh
KAFKA_BOOTSTRAP=localhost:19092,localhost:19093,localhost:19094
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_CONNECT_URL=http://localhost:8083
```

Логирование — через `LOG_LEVEL=debug|info|warn|error` (по умолчанию info, текст в stderr).

## Структура

```
lectures/
├── go.work                     # workspace, объединяет все модули лекций
├── README.md                   # этот файл
├── Makefile                    # делегирует make внутрь лекций
├── internal/                   # shared helpers — kafka client, env, runctx, log
│   ├── kafka/
│   ├── config/
│   ├── runctx/
│   └── log/
├── 01-foundations/             # модуль 01
│   └── 01-01-architecture-and-kraft/
│       ├── go.mod
│       ├── README.md
│       ├── Makefile
│       └── cmd/<binary>/main.go
├── 02-producer/                # модуль 02
├── ...
└── 09-use-cases/               # сквозные use case'ы (отдельный README, integration test)
```

Внутри лекции почти всегда один-два бинарника в `cmd/`. Use case'ы крупнее — там несколько сервисов, proto-схемы, иногда `docker-compose.override.yml` под дополнительный Postgres/ClickHouse/ES.

## Shared helpers

Чтобы не копировать в каждой лекции один и тот же бойлерплейт, общие куски лежат в `internal/`:

- `internal/kafka.NewClient(opts ...kgo.Opt)` — kgo.Client с дефолтами курса (SeedBrokers из env, ClientID `lectures`, разумные таймауты)
- `internal/kafka.NewAdmin()` — admin-клиент для CreateTopic/DescribeTopics/Lag
- `internal/config.MustEnv(name)`, `EnvOr(name, default)` — обёртки над os.Getenv
- `internal/runctx.New()` — context, отменяемый по SIGINT/SIGTERM
- `internal/log.New()` — slog logger с уровнем из LOG_LEVEL

Пользоваться так (псевдокод из будущей лекции):

```go
ctx, cancel := runctx.New()
defer cancel()

cl, err := kafkactl.NewClient()
if err != nil { panic(err) }
defer cl.Close()
```

Каждая лекция в своём `go.mod` пишет:

```
require (
    github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0
    github.com/twmb/franz-go v1.21.0
)
```

Workspace разруливает локальный путь к `internal` через `go.work`.

## Оглавление

Курс идёт по уровням сложности. Модули 01–04 — обязательная база, дальше можно прыгать в любом порядке (но 05 нужен перед 09-03/09-04, и 07-04 опирается на 04-03).

### 01 — Основы

Что такое Kafka вообще: брокер, контроллер, топик, партиция, репликация, offset, retention. Первый продьюсер и консьюмер на franz-go. После этого модуля у тебя в голове есть рабочая модель — дальше можно копать вглубь.

- [01-01 — Architecture & KRaft](lectures/01-foundations/01-01-architecture-and-kraft/README.md)
- [01-02 — Topics & Partitions](lectures/01-foundations/01-02-topics-and-partitions/README.md)
- [01-03 — Replication & ISR](lectures/01-foundations/01-03-replication-and-isr/README.md)
- [01-04 — Offsets & Retention](lectures/01-foundations/01-04-offsets-and-retention/README.md)
- [01-05 — First Producer](lectures/01-foundations/01-05-first-producer/README.md)
- [01-06 — First Consumer](lectures/01-foundations/01-06-first-consumer/README.md)

### 02 — Продьюсер

Ключи и партиционирование (где живёт one-key-one-partition гарантия), acks и durability, идемпотентность, батчинг и компрессия, классы ошибок и headers.

- [02-01 — Keys & Partitioning](lectures/02-producer/02-01-keys-and-partitioning/README.md)
- [02-02 — Acks & Durability](lectures/02-producer/02-02-acks-and-durability/README.md)
- [02-03 — Idempotent Producer](lectures/02-producer/02-03-idempotent-producer/README.md)
- [02-04 — Batching & Throughput](lectures/02-producer/02-04-batching-and-throughput/README.md)
- [02-05 — Errors, Retries & Headers](lectures/02-producer/02-05-errors-retries-headers/README.md)

### 03 — Консьюмер

Группы и ребалансы (включая cooperative-sticky), коммиты offset'ов, гарантии обработки (at-most/at-least/exactly-once на стороне консьюмера), error handling с retry-топиками и DLQ, конкурентность и lag.

- [03-01 — Consumer Groups & Rebalance](lectures/03-consumer/03-01-groups-and-rebalance/README.md)
- [03-02 — Offset Commits](lectures/03-consumer/03-02-offset-commits/README.md)
- [03-03 — Processing Guarantees](lectures/03-consumer/03-03-processing-guarantees/README.md)
- [03-04 — Error Handling](lectures/03-consumer/03-04-error-handling/README.md)
- [03-05 — Concurrency & Lag](lectures/03-consumer/03-05-concurrency-and-lag/README.md)

### 04 — Надёжность

Транзакции и exactly-once semantics, consume-process-produce паттерн, transactional outbox, retry/DLQ deep dive, доставка во внешние системы (HTTP courier с CB и backpressure через PauseFetchPartitions).

- [04-01 — Transactions & EOS](lectures/04-reliability/04-01-transactions-and-eos/README.md)
- [04-02 — Consume-Process-Produce](lectures/04-reliability/04-02-consume-process-produce/README.md)
- [04-03 — Outbox Pattern](lectures/04-reliability/04-03-outbox-pattern/README.md)
- [04-04 — Retry & DLQ Deep Dive](lectures/04-reliability/04-04-retry-and-dlq/README.md)
- [04-05 — External Delivery](lectures/04-reliability/04-05-external-delivery/README.md)

### 05 — Контракты

Зачем вообще схемы (JSON vs Avro vs Protobuf), Protobuf в Go через buf, Schema Registry с magic byte и schema_id, эволюция схем (BACKWARD/FORWARD/FULL и что такое breaking change в protobuf).

- [05-01 — Why Contracts & Wire Formats](lectures/05-contracts/05-01-why-contracts-and-wire-formats/README.md)
- [05-02 — Protobuf in Go](lectures/05-contracts/05-02-protobuf-in-go/README.md)
- [05-03 — Schema Registry](lectures/05-contracts/05-03-schema-registry/README.md)
- [05-04 — Schema Evolution](lectures/05-contracts/05-04-schema-evolution/README.md)

### 06 — Паттерны коммуникации

Тут самое интересное для тех, кто проектирует системы. gRPC basics и streaming, синхрон против асинхрона (decision matrix), гибрид gRPC + Kafka на одном сервисе, saga (choreography vs orchestration).

- [06-01 — gRPC Basics](lectures/06-communication-patterns/06-01-grpc-basics/README.md)
- [06-02 — gRPC Streaming](lectures/06-communication-patterns/06-02-grpc-streaming/README.md)
- [06-03 — Sync vs Async](lectures/06-communication-patterns/06-03-sync-vs-async/README.md)
- [06-04 — Hybrid: gRPC + Kafka](lectures/06-communication-patterns/06-04-hybrid-grpc-and-kafka/README.md)
- [06-05 — Saga: Choreography vs Orchestration](lectures/06-communication-patterns/06-05-saga-choreography/README.md)

### 07 — Streams и Connect

Stream-processing концепции (event-time, windowing, watermark, late events), реализация word-count на franz-go + Pebble (заменитель Kafka Streams для Go), Kafka Connect, Debezium CDC.

- [07-01 — Stream Processing Concepts](lectures/07-streams-and-connect/07-01-stream-processing-concepts/README.md)
- [07-02 — Stream Processing in Go (franz-go + Pebble)](lectures/07-streams-and-connect/07-02-stream-processing-in-go/README.md)
- [07-03 — Kafka Connect](lectures/07-streams-and-connect/07-03-kafka-connect/README.md)
- [07-04 — Debezium CDC](lectures/07-streams-and-connect/07-04-debezium-cdc/README.md)

### 08 — Эксплуатация

Мониторинг и метрики (kminion как exporter, Grafana dashboard), retention vs compaction на практике, sizing и tuning топиков под профиль нагрузки, troubleshooting runbook на 10–12 типовых проблем.

- [08-01 — Monitoring & Metrics](lectures/08-operations/08-01-monitoring-and-metrics/README.md)
- [08-02 — Retention & Compaction](lectures/08-operations/08-02-retention-and-compaction/README.md)
- [08-03 — Sizing & Tuning](lectures/08-operations/08-03-sizing-and-tuning/README.md)
- [08-04 — Troubleshooting Runbook](lectures/08-operations/08-04-troubleshooting-runbook/README.md)

### 09 — Use cases

Сквозные сценарии — связывают всё, что было выше, в работающие приложения с integration-тестами.

- [09-01 — Microservices Communication](lectures/09-use-cases/01-microservices-comm/README.md) (3 сервиса × 2 ноды, gRPC + outbox + Kafka)
- [09-02 — Push Notifications](lectures/09-use-cases/02-push-notifications/README.md) (router → Firebase/APNs/webhook с retry+DLQ, CB, HMAC, replay, integration test)
- [09-03 — Postgres → ClickHouse с анонимизацией](lectures/09-use-cases/03-pg-to-clickhouse/README.md) (Debezium + Go-anonymizer с декларативным YAML + ClickHouse Sink, ReplacingMergeTree(cdc_lsn) для дедупа, integration test)
- [09-04 — Postgres → Elasticsearch](lectures/09-use-cases/04-pg-to-elasticsearch/README.md) (Debezium + Confluent ES Sink через ExtractNewRecordState SMT, index template, blue-green reindex через alias rotation, integration test)

## Как добавить новую лекцию

Внутри модуля создаёшь папку формата `<MM>-<short-name>/`, кладёшь `go.mod` с module path `github.com/dsbasko/kafka-sandbox/lectures/<NN-module>/<MM-short>`, добавляешь `use ./<NN-module>/<MM-short>` в `lectures/go.work`, дальше README + Makefile + cmd. Раздел оглавления выше обновляешь руками — это чек-лист, что лекция готова, а не просто папка.

## Совместимость и что вне scope

Курс — учебный sandbox. Production-конфиги (security, ACL, mTLS, multi-DC репликация, MirrorMaker2) тут не разбираются — они требуют реальной инфраструктуры и выходят за рамки локальной машины. Альтернативные клиенты (Sarama, confluent-kafka-go) тоже не разбираются: курс целенаправленно один — franz-go.

Если что-то сломалось на стенде — смотри `docker-compose.yml` и логи контейнеров (`docker compose logs kafka-1`).
