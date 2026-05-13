# i18n Glossary — RU ↔ EN

Translation reference for the `kafka-cookbook` course. The course is written in Russian; English is the second first-class language. This glossary keeps terminology consistent across UI strings, READMEs, and lecture content.

Source of truth: this file. When a translation question comes up — extend this file, then update the code.

## Terminology mapping

Russian terms map to English on a one-to-one basis. Pick the English form below; do not invent synonyms.

| Russian | English | Notes |
|---|---|---|
| брокер | broker | |
| партиция | partition | |
| продьюсер | producer | |
| консьюмер | consumer | |
| ребаланс | rebalance | also: rebalancing (process noun) |
| смещение | offset | |
| удержание | retention | not "retainment", not "keep-time" |
| идемпотентный | idempotent | |
| идемпотентность | idempotency | |
| транзакция | transaction | |
| контракт | contract | as in schema contract |
| схема | schema | as in Avro/Protobuf schema |
| эволюция схем | schema evolution | |
| ошибка | error | not "fault", not "failure" unless context demands it |
| повтор | retry | verb: to retry; noun: a retry |
| политика повторов | retry policy | |
| дроссель | backpressure | the term "backpressure" already loaned in Russian, keep direct |
| backpressure | backpressure | already English, keep |
| контроллер | controller | KRaft controller specifically |
| реплика | replica | |
| репликация | replication | |
| лидер | leader | leader partition / leader broker |
| фолловер | follower | |
| надёжность | reliability | not "robustness" |
| доставка | delivery | as in at-least-once delivery |
| гарантии доставки | delivery guarantees | |
| подтверждение | acknowledgement | abbrev: ack |
| коммит | commit | offset commit |
| оффсет-коммит | offset commit | |
| группа консьюмеров | consumer group | |
| лаг | lag | consumer lag |
| тема / топик | topic | prefer "topic" — the course uses it directly |
| ключ сообщения | message key | |
| значение сообщения | message value | not "payload" unless contrasting with metadata |
| заголовок | header | message header |
| сериализация | serialization | |
| десериализация | deserialization | |
| формат | format | |
| компакция | compaction | log compaction |
| сегмент | segment | log segment |
| watermark | watermark | already English, keep |
| окно | window | streams windowing |
| оконная агрегация | windowed aggregation | |
| состояние | state | stateful stream processing |
| stateful / stateless | stateful / stateless | already English |
| лог | log | as in append-only log |
| append-only | append-only | already English |
| outbox | outbox | already English, keep |
| CDC | CDC | abbreviation, keep |
| наблюдаемость | observability | |
| метрика | metric | |
| трейс | trace | |
| трассировка | tracing | |
| лог (запись) | log entry | when ambiguous with append-only log |
| дедупликация | deduplication | |
| партицирование | partitioning | |
| ключ партицирования | partition key | |
| балансировка | balancing | load balancing |
| прогрев | warm-up | as in warm-up phase |
| прогон | run | as in dry run, test run |
| сквозной | end-to-end | end-to-end test |
| тестирование | testing | |
| стенд | sandbox | the local Kafka sandbox |
| окружение | environment | env var = environment variable |
| переменная окружения | environment variable | |

## Non-translatable names

Keep verbatim in any language — these are product names, libraries, protocols, or established acronyms.

Libraries and tools:

- `franz-go`
- `kgo`, `kadm`, `kmsg` (franz-go subpackages)
- `buf` (Protobuf tooling)

Kafka ecosystem:

- Kafka
- Apache Kafka
- KRaft
- ZooKeeper
- Kafka Connect
- Kafka UI
- Schema Registry
- Confluent
- Confluent Schema Registry
- Debezium

Protocols and formats:

- gRPC
- Protobuf
- Avro
- JSON Schema
- JSON
- YAML
- HTTP
- TCP
- SASL
- SSL / TLS
- mTLS
- OAuth

Storage and infra:

- Postgres / PostgreSQL
- ClickHouse
- Elasticsearch
- Pebble
- Docker
- Docker Compose

Languages and runtimes:

- Go
- Node.js
- TypeScript

Code identifiers, file names, command names, env var names, CLI flags — keep verbatim regardless of language (e.g., `KAFKA_BOOTSTRAP`, `make lecture`, `go.mod`, `docker-compose.yml`).

## Style rules

Apply the same voice across English translations as the Russian source uses.

1. Imperative mood for instructions. "Run the producer", not "You should run the producer" or "The producer can be run".
2. Direct sentences. Cut hedging. "This breaks on retries" beats "This may potentially cause issues during retry scenarios".
3. Active voice by default. Use passive only when the actor is genuinely irrelevant.
4. No AI tells. Avoid "delve", "leverage", "robust", "seamless", "comprehensive", "holistic", "facilitate", "in the realm of", "it's worth noting that", "furthermore", "moreover" — see the `humanizer` skill notes for the full list.
5. No filler. Drop "basically", "essentially", "simply", "just" unless they carry meaning.
6. Concrete over abstract. "Retention is 7 days" beats "Retention is configured for a meaningful period".
7. Match the source structure. Do not split paragraphs, merge them, or reorder sections without a reason.
8. Code blocks, links, image refs, and frontmatter pass through unchanged. Only comments inside code are translated when they exist in the source — but the course strips Go comments, so this rarely applies.
9. Numbers and units stay in source form: `4 GB`, `7 days`, `100 messages/sec`. Do not localise number formats.
10. Russian quotes «...» become English `"..."`. Russian dashes ­— stay as em dashes in English too, but used sparingly.
11. Headings use sentence case in English: "Getting started", not "Getting Started" — except when the source heading is a proper noun or product name.
12. The `kafka-cookbook` brand stays lowercase-hyphenated everywhere.

## Tone calibration

Russian source tone (from the root README): direct, no sugar, runs on a real machine, observable effect on a real cluster. The English version keeps the same posture — no marketing softening, no apologies for difficulty, no "let's explore together" framing.

Counter-examples (what NOT to write):

- "In this comprehensive lesson, we'll embark on a journey through the fascinating world of Kafka partitions" — drop "comprehensive", "embark on a journey", "fascinating world".
- "It's important to note that producers can potentially face issues" — drop the hedge stack; say what the issue is.
- "Leveraging the power of franz-go, we seamlessly integrate..." — drop "leveraging", "seamlessly". Say: "franz-go handles X".

Good examples (target voice):

- "Producers send messages to a topic. Brokers append them to a partition. That's the whole story for the happy path."
- "Retries break ordering unless you pin the partition. Pin it."
- "The sandbox runs three brokers in KRaft mode. No ZooKeeper."
