# Kafka Cookbook — учебный курс на Go

## Что это

Учебный курс по Apache Kafka на Go: от первого продьюсера до production-grade паттернов (transactions, outbox, CDC через Debezium, гибрид gRPC + Kafka). Поверх локального docker-compose стенда (Kafka 4.2.0 в KRaft mode, Schema Registry, Kafka Connect, Kafka UI) лежит 9 модулей и 39 единиц материала на Go и [twmb/franz-go](https://github.com/twmb/franz-go) — каждая лекция запускается на твоей машине через `make run` и оставляет наблюдаемый эффект на стенде.

## Стек

- Go 1.26
- Apache Kafka 4.2.0 (KRaft mode, без ZooKeeper)
- 3 брокера `apache/kafka:4.2.0` в combined mode на одном `cluster.id`, `RF=3`, `min.insync.replicas=2`
- [twmb/franz-go](https://github.com/twmb/franz-go) v1.21.0 — Kafka-клиент
- Protobuf и gRPC (кодогенерация через buf)
- Confluent Schema Registry 8.0.0 — для Avro/Protobuf
- Confluent Kafka Connect 8.0.0 + Debezium — для CDC/ETL коннекторов
- Kafka UI ([kafbat](https://github.com/kafbat/kafka-ui)) — визуальное наблюдение
- Postgres, ClickHouse, Elasticsearch и mock-сервисы — для use case'ов модуля 09

## Quick start

```bash
docker compose up -d
# ожидание ~120s — пока 3 брокера соберут KRaft quorum, поднимутся UI/SR/Connect
docker compose ps
```

После того как все сервисы перешли в `healthy`/`running`, открыть Kafka UI: <http://localhost:8080>. Кластер `local-kraft` должен быть зелёным, в нём — 3 брокера, привязаны Schema Registry и Kafka Connect.

Остановка с сохранением данных:

```bash
docker compose down
```

Полная очистка. Стенд хранит данные брокеров в bind-mount'ах `./.temp/data/kafka-{1,2,3}`, поэтому `docker compose down -v` сам по себе их не удалит — нужен явный `rm -rf`. При следующем `up` кластер инициализируется заново с тем же `CLUSTER_ID` из `docker-compose.yml` — регенерировать UUID не нужно:

```bash
docker compose down -v
rm -rf ./.temp/data
```

## Endpoints

| Сервис          | Назначение                              | С хоста                                                              | Внутри docker-сети                          |
|-----------------|-----------------------------------------|----------------------------------------------------------------------|---------------------------------------------|
| Kafka bootstrap | продьюсеры / консьюмеры                 | `127.0.0.1:19092`, `127.0.0.1:19093`, `127.0.0.1:19094`              | `kafka-1:9092`, `kafka-2:9092`, `kafka-3:9092` |
| Kafka UI        | веб-интерфейс                           | <http://localhost:8080>                                              | `http://kafka-ui:8080`                      |
| Schema Registry | Avro/Protobuf-схемы                     | <http://localhost:8081>                                              | `http://schema-registry:8081`               |
| Kafka Connect   | REST API для коннекторов                | <http://localhost:8083>                                              | `http://kafka-connect:8083`                 |

EXTERNAL listener `19092/19093/19094` — для клиентов с хоста (kcat, IDE, Go/Python приложения). INTERNAL listener `9092` — для межброкерной репликации и сервисов в той же docker-сети (UI/SR/Connect).

Bootstrap для клиентов и URL'ы зависимостей — переменные окружения, дефолты уже подходят для локального стенда:

```sh
KAFKA_BOOTSTRAP=localhost:19092,localhost:19093,localhost:19094
SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_CONNECT_URL=http://localhost:8083
```

Логирование — через `LOG_LEVEL=debug|info|warn|error` (по умолчанию `info`, текст в stderr).

## Как пользоваться курсом

Сначала подними стенд (`docker compose up -d`). Дальше из корня репозитория:

```sh
make list                                                    # дерево лекций
make lecture L=01-foundations/01-01-architecture-and-kraft  # запустить конкретную
```

Каждая лекция — отдельный Go-модуль со своим `go.mod`, README на русском и Makefile с целями `run`, `topic-create`, плюс что-нибудь специфичное (`kill-broker`, `proto-gen`, `connector-create` — зависит от темы).

Для лекции 07-04 (Debezium CDC) дополнительно нужен набор Connect plugins: `make connect-install-plugins` (Debezium, ClickHouse Sink, Elasticsearch Sink). Подробности — в [`connect-plugins/README.md`](connect-plugins/README.md).

## Оглавление курса

Курс идёт по уровням сложности. Модули 01–04 — обязательная база, дальше можно прыгать в любом порядке (но 05 нужен перед 09-03/09-04, и 07-04 опирается на 04-03).

### 01 — Основы

Что такое Kafka вообще: брокер, контроллер, топик, партиция, репликация, offset, retention. Первый продьюсер и консьюмер на franz-go. После этого модуля у тебя в голове есть рабочая модель — дальше можно копать вглубь.

- [01-01 — Architecture & KRaft](01-foundations/01-01-architecture-and-kraft/README.md)
- [01-02 — Topics & Partitions](01-foundations/01-02-topics-and-partitions/README.md)
- [01-03 — Replication & ISR](01-foundations/01-03-replication-and-isr/README.md)
- [01-04 — Offsets & Retention](01-foundations/01-04-offsets-and-retention/README.md)
- [01-05 — First Producer](01-foundations/01-05-first-producer/README.md)
- [01-06 — First Consumer](01-foundations/01-06-first-consumer/README.md)

### 02 — Продьюсер

Ключи и партиционирование (где живёт one-key-one-partition гарантия), acks и durability, идемпотентность, батчинг и компрессия, классы ошибок и headers.

- [02-01 — Keys & Partitioning](02-producer/02-01-keys-and-partitioning/README.md)
- [02-02 — Acks & Durability](02-producer/02-02-acks-and-durability/README.md)
- [02-03 — Idempotent Producer](02-producer/02-03-idempotent-producer/README.md)
- [02-04 — Batching & Throughput](02-producer/02-04-batching-and-throughput/README.md)
- [02-05 — Errors, Retries & Headers](02-producer/02-05-errors-retries-headers/README.md)

### 03 — Консьюмер

Группы и ребалансы (включая cooperative-sticky), коммиты offset'ов, гарантии обработки (at-most/at-least/exactly-once на стороне консьюмера), error handling с retry-топиками и DLQ, конкурентность и lag.

- [03-01 — Consumer Groups & Rebalance](03-consumer/03-01-groups-and-rebalance/README.md)
- [03-02 — Offset Commits](03-consumer/03-02-offset-commits/README.md)
- [03-03 — Processing Guarantees](03-consumer/03-03-processing-guarantees/README.md)
- [03-04 — Error Handling](03-consumer/03-04-error-handling/README.md)
- [03-05 — Concurrency & Lag](03-consumer/03-05-concurrency-and-lag/README.md)

### 04 — Надёжность

Транзакции и exactly-once semantics, consume-process-produce паттерн, transactional outbox, retry/DLQ deep dive, доставка во внешние системы (HTTP courier с CB и backpressure через PauseFetchPartitions).

- [04-01 — Transactions & EOS](04-reliability/04-01-transactions-and-eos/README.md)
- [04-02 — Consume-Process-Produce](04-reliability/04-02-consume-process-produce/README.md)
- [04-03 — Outbox Pattern](04-reliability/04-03-outbox-pattern/README.md)
- [04-04 — Retry & DLQ Deep Dive](04-reliability/04-04-retry-and-dlq/README.md)
- [04-05 — External Delivery](04-reliability/04-05-external-delivery/README.md)

### 05 — Контракты

Зачем вообще схемы (JSON vs Avro vs Protobuf), Protobuf в Go через buf, Schema Registry с magic byte и schema_id, эволюция схем (BACKWARD/FORWARD/FULL и что такое breaking change в protobuf).

- [05-01 — Why Contracts & Wire Formats](05-contracts/05-01-why-contracts-and-wire-formats/README.md)
- [05-02 — Protobuf in Go](05-contracts/05-02-protobuf-in-go/README.md)
- [05-03 — Schema Registry](05-contracts/05-03-schema-registry/README.md)
- [05-04 — Schema Evolution](05-contracts/05-04-schema-evolution/README.md)

### 06 — Паттерны коммуникации

Тут самое интересное для тех, кто проектирует системы. gRPC basics и streaming, синхрон против асинхрона (decision matrix), гибрид gRPC + Kafka на одном сервисе, saga (choreography vs orchestration).

- [06-01 — gRPC Basics](06-communication-patterns/06-01-grpc-basics/README.md)
- [06-02 — gRPC Streaming](06-communication-patterns/06-02-grpc-streaming/README.md)
- [06-03 — Sync vs Async](06-communication-patterns/06-03-sync-vs-async/README.md)
- [06-04 — Hybrid: gRPC + Kafka](06-communication-patterns/06-04-hybrid-grpc-and-kafka/README.md)
- [06-05 — Saga: Choreography vs Orchestration](06-communication-patterns/06-05-saga-choreography/README.md)

### 07 — Streams и Connect

Stream-processing концепции (event-time, windowing, watermark, late events), реализация word-count на franz-go + Pebble (заменитель Kafka Streams для Go), Kafka Connect, Debezium CDC.

- [07-01 — Stream Processing Concepts](07-streams-and-connect/07-01-stream-processing-concepts/README.md)
- [07-02 — Stream Processing in Go (franz-go + Pebble)](07-streams-and-connect/07-02-stream-processing-in-go/README.md)
- [07-03 — Kafka Connect](07-streams-and-connect/07-03-kafka-connect/README.md)
- [07-04 — Debezium CDC](07-streams-and-connect/07-04-debezium-cdc/README.md)

### 08 — Эксплуатация

Мониторинг и метрики (kminion как exporter, Grafana dashboard), retention vs compaction на практике, sizing и tuning топиков под профиль нагрузки, troubleshooting runbook на 10–12 типовых проблем.

- [08-01 — Monitoring & Metrics](08-operations/08-01-monitoring-and-metrics/README.md)
- [08-02 — Retention & Compaction](08-operations/08-02-retention-and-compaction/README.md)
- [08-03 — Sizing & Tuning](08-operations/08-03-sizing-and-tuning/README.md)
- [08-04 — Troubleshooting Runbook](08-operations/08-04-troubleshooting-runbook/README.md)

### 09 — Use cases

Сквозные сценарии — связывают всё, что было выше, в работающие приложения с integration-тестами.

- [09-01 — Microservices Communication](09-use-cases/01-microservices-comm/README.md) (3 сервиса × 2 ноды, gRPC + outbox + Kafka)
- 09-02 — Push Notifications (router → Firebase/APNs/webhook с retry+DLQ) — *запланировано, не реализовано*
- 09-03 — Postgres → ClickHouse с анонимизацией (Debezium + Go-anonymizer + ClickHouse Sink) — *запланировано, не реализовано*
- 09-04 — Postgres → Elasticsearch (Debezium + ES Sink, blue-green reindex) — *запланировано, не реализовано*

## Структура репозитория

```
kafka-cookbook/
├── docker-compose.yml          # стенд: 3× Kafka + UI + Schema Registry + Connect
├── Makefile                    # точка входа: list / lecture / sync / build / connect-*
├── go.work                     # workspace, объединяет internal и все модули лекций
├── README.md                   # этот файл
├── connect-plugins/            # Debezium / ClickHouse Sink / ES Sink (через make)
├── internal/                   # shared helpers — kafka client, env, runctx, log
│   ├── kafka/
│   ├── config/
│   ├── runctx/
│   └── log/
├── 01-foundations/             # модуль 01 (6 подлекций)
│   └── 01-01-architecture-and-kraft/
│       ├── go.mod
│       ├── README.md
│       ├── Makefile
│       └── cmd/<binary>/main.go
├── 02-producer/                # модуль 02 (5)
├── 03-consumer/                # модуль 03 (5)
├── 04-reliability/             # модуль 04 (5)
├── 05-contracts/               # модуль 05 (4)
├── 06-communication-patterns/  # модуль 06 (5)
├── 07-streams-and-connect/     # модуль 07 (4)
├── 08-operations/              # модуль 08 (4)
└── 09-use-cases/               # модуль 09 (1 реализован, 3 запланированы)
```

Внутри лекции почти всегда один-два бинарника в `cmd/`. Use case'ы крупнее — там несколько сервисов, proto-схемы, иногда `docker-compose.override.yml` под дополнительный Postgres/ClickHouse/ES.

## Shared helpers

Чтобы не копировать в каждой лекции один и тот же бойлерплейт, общие куски лежат в `internal/`:

- `internal/kafka.NewClient(opts ...kgo.Opt)` — kgo.Client с дефолтами курса (SeedBrokers из env, ClientID `lectures`, разумные таймауты)
- `internal/kafka.NewAdmin()` — admin-клиент для CreateTopic/DescribeTopics/Lag
- `internal/config.MustEnv(name)`, `EnvOr(name, default)` — обёртки над os.Getenv
- `internal/runctx.New()` — context, отменяемый по SIGINT/SIGTERM
- `internal/log.New()` — slog logger с уровнем из LOG_LEVEL

Каждая лекция в своём `go.mod` пишет:

```
require (
    github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0
    github.com/twmb/franz-go v1.21.0
)
```

Workspace разруливает локальный путь к `internal` через `go.work`. Исторический module path сохранён намеренно — это строка-идентификатор Go-модуля, она не зависит от физического расположения папок и не требует переименования при уплощении структуры.

## Smoke tests

Команды для проверки, что стенд жив и настроен правильно.

### KRaft quorum

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status
# 3 voters, healthy quorum, leader выбран
```

### EXTERNAL listener виден с хоста

```bash
kcat -b 127.0.0.1:19092 -L | head -30
# или без kcat:
docker run --rm --network host apache/kafka:4.2.0 \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:19092
```

### Топик с RF=3 на 3 нодах

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create --topic smoke --partitions 3 --replication-factor 3

docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic smoke
# реплики на всех 3 нодах, ISR=3
```

### Schema Registry: регистрация и чтение Avro-схемы

```bash
curl -sf http://localhost:8081/subjects
# []

curl -sf -X POST http://localhost:8081/subjects/test-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}'

curl -sf http://localhost:8081/subjects/test-value/versions/1
curl -sf -X DELETE http://localhost:8081/subjects/test-value
```

### Kafka Connect plugins

```bash
curl -sf http://localhost:8083/connector-plugins | jq 'length'
# >= 3 (встроенные Mirror{Source,Checkpoint,Heartbeat}Connector)
```

### Kafka UI

```bash
curl -sf http://localhost:8080/api/clusters | jq
# массив с одним кластером local-kraft, status=ONLINE, brokerCount=3
```

И отдельно — открыть <http://localhost:8080> в браузере.

## Failure scenarios

Стенд настроен на реалистичную отказоустойчивость: `RF=3`, `min.insync.replicas=2`, `acks=all` со стороны клиентов. Это позволяет переживать падение одной ноды без остановки записи.

### Отказ одной ноды

Предусловие — топик `smoke` создан (см. smoke test выше); cleanup из секции ниже запускайте только после прогонки этого сценария.

```bash
docker compose stop kafka-2

# через ~10s проверяем quorum — он остаётся живым (2 voters >= majority of 3)
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status

# запись с acks=all продолжает проходить (ISR=2 >= min.ISR=2)
echo "msg1" | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 --topic smoke \
  --producer-property acks=all

# возвращаем ноду
docker compose start kafka-2
docker compose ps
# ждём, пока kafka-2 снова healthy; ISR-recovery — автоматически
```

**Что не покроется этим стендом:** одновременный отказ двух нод из трёх. При `min.insync.replicas=2` запись с `acks=all` остановится с `NotEnoughReplicasException`, а KRaft quorum (1 voter из 3) будет ниже majority — записи в metadata тоже остановятся. Это ожидаемое поведение для реалистичной отказоустойчивости.

### Cleanup после smoke-прогонов

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --delete --topic smoke
```

## Troubleshooting

**`docker compose up` зависает на старте брокеров.**
Чаще всего — невалидный или не-одинаковый `CLUSTER_ID` в `docker-compose.yml` (поле `x-kafka-common-env.CLUSTER_ID`). Все три брокера должны стартовать с одним и тем же `CLUSTER_ID` (base64 UUID). Если значение меняли после первого старта — данные в bind-mount'ах несовместимы; данные хранятся в `./.temp/data/`, их нужно удалить вручную (`docker compose down -v` бинд-маунты не трогает):

```bash
docker compose down -v
rm -rf ./.temp/data
docker compose up -d
```

**Брокеры стартуют по одному и healthcheck зависает.**
Healthcheck `kafka-metadata-quorum.sh ... describe --status` требует собранный quorum. Если поднимать брокеров по одному (`up -d kafka-1`, потом `up -d kafka-2`), первый зависнет в ожидании остальных voters. Запускать вместе:

```bash
docker compose up -d kafka-1 kafka-2 kafka-3
# или просто
docker compose up -d
```

**Клиент с хоста подключается к bootstrap, но висит на metadata.**
Почти всегда — неправильный `EXTERNAL` advertised listener: порт проброса (`19092/19093/19094`) не совпадает с тем, что брокер сам себе анонсирует. Проверить:

```bash
docker exec kafka-1 cat /var/lib/kafka/data/meta.properties
docker compose config | grep ADVERTISED_LISTENERS
```

В `docker-compose.yml`: `kafka-1` → `EXTERNAL://127.0.0.1:19092`, `kafka-2` → `127.0.0.1:19093`, `kafka-3` → `127.0.0.1:19094`. Любой mismatch с проброшенными портами — приведёт к зависанию клиента.

**Schema Registry падает на старте с `NOT_ENOUGH_REPLICAS`.**
Контроллер ещё не успел выбрать лидера для `__cluster_metadata` / системных топиков. В compose это закрывается `depends_on: condition: service_healthy` для всех 3 брокеров и `start_period: 45s` у healthcheck'а. Если всё равно падает — увеличить `start_period` или `retries`.

**Внутренние топики Connect (`_connect-configs`, `_connect-offsets`, `_connect-status`) создались с RF=1.**
Значит `CONNECT_*_REPLICATION_FACTOR` env-переменные не подхватились (опечатка в имени, неверный регистр). Проверить:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic _connect-configs
# ReplicationFactor: 3 — норма; 1 — баг
```

Лечится удалением топиков и рестартом Connect — он пересоздаст их с правильным RF при старте:

```bash
for t in _connect-configs _connect-offsets _connect-status; do
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 --delete --topic "$t"
done
docker compose restart kafka-connect
```

**После `docker compose down -v` + `rm -rf ./.temp/data` стенд не поднимается с прежним `CLUSTER_ID`.**
Bind-mount'ы очищены — это нормально, кластер собирается заново. Если `CLUSTER_ID` в `docker-compose.yml` остался прежним — всё ок, новый cluster инициализируется с тем же UUID. Если меняли UUID и часть данных выжила в `./.temp/data/kafka-N` — будет конфликт. Решение: удалить `./.temp/data` целиком, не точечно по нодам.

## Out of scope

В курсе намеренно не покрыто:

- **SASL / SSL / mTLS** — стенд использует `PLAINTEXT` на всех listener'ах. ACL, JAAS, SCRAM-SHA-512 требуют отдельной инфраструктуры.
- **Multi-DC / MirrorMaker2** — single-cluster стенд. Cross-DC репликация требует второго кластера.
- **Tiered storage / KRaft-only ремонтные процедуры** (replica reassignment, log compaction tuning) — учебный стенд держит дефолты.
- **Альтернативные Go-клиенты** (Sarama, confluent-kafka-go) — курс целенаправленно один: franz-go.
- **Production security и compliance** (RBAC, audit log, encryption at rest) — выходит за рамки локальной машины.

Метрики/Grafana, Debezium и gRPC-примеры на Go — внутри курса: см. модули 08 (мониторинг и эксплуатация), 07-04 (CDC), 06 (gRPC + Kafka). Use case'ы 09-02/09-03/09-04 (push notifications, PG → ClickHouse, PG → Elasticsearch) пока запланированы, но ещё не реализованы.
