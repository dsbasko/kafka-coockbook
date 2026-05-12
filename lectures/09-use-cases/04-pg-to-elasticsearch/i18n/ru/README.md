# 09-04 — Postgres → Elasticsearch

Декларативный ETL под поиск. Слева Postgres с товарным каталогом, статьями и пользователями. Справа Elasticsearch — туда нужно затащить эти же данные так, чтобы по ним работал full-text search. Между ними — ничего своего, только Debezium и Elasticsearch Sink connector. Go-код в этом use case'е есть, но он диагностический: db-loader и search-tester не участвуют в pipeline'е, ими удобно проверять, что всё доехало.

Это контраст к [Postgres → ClickHouse с анонимизацией](../../../03-pg-to-clickhouse/i18n/ru/README.md). Там между Debezium и Sink'ом стоял анонимизатор на Go — ему деваться было некуда, маски PII требуют логики. Тут логики нет: схема Postgres почти один-в-один ложится на JSON-документ ES, а трансформации формата (отвернуть Debezium-конверт, выдернуть ключ, переименовать топик в индекс) тянут стандартные Single Message Transforms внутри Connect. Главное достоинство — pipeline можно собрать и поддерживать без отдельного сервиса, который надо деплоить и мониторить.

## Что собираем

```
Postgres (15443)
   │
   │  WAL (logical replication, slot)
   ▼
kafka-connect: Debezium PostgresConnector
   │
   ▼
search.public.products / search.public.articles / search.public.users
   │
   │  ExtractNewRecordState (unwrap)
   │  ExtractField$Key      (id из ключа в _id)
   │  RegexRouter           (search.public.X → X_v1)
   ▼
kafka-connect: ES Sink connector (v1)
   │
   ▼
Elasticsearch (19200) → indices products_v1 / articles_v1 / users_v1
```

Сравни с [Postgres → ClickHouse с анонимизацией](../../../03-pg-to-clickhouse/i18n/ru/README.md): то же количество звеньев в Connect'е, только вместо ClickHouse Sink'а — ES Sink, и плюс цепочка SMT, потому что ES любит плоский документ, а Debezium шлёт обёрнутый. Postgres-схема, publication, replication slot, RF — без изменений.

## Зачем здесь Single Message Transforms

Debezium кладёт в Kafka событие в формате envelope:

```json
{
  "before": null,
  "after": {"id": 1, "name": "Product 1 alpha", "price_cents": 200, ...},
  "source": {"lsn": 12345, "ts_ms": ...},
  "op": "c"
}
```

ES Sink из коробки положит этот объект как есть. Получится индекс с полем `before`, `after`, `source`, `op` — не то, что ожидаешь, когда пишешь `match: {name: "alpha"}`. SMT в этом use case'е делают три простые вещи.

**ExtractNewRecordState** — из envelope достаёт `after` и кладёт в корень. Для DELETE-операций (`op=d`) `after` пустой, и Debezium ставит value=null — это в ES Sink триггерит `behavior.on.null.values=delete`, документ исчезает.

**ExtractField$Key** — Debezium шлёт ключ как объект `{"id": 1}`, а ES хочет в `_id` строку или число. SMT берёт поле `id`.

**RegexRouter** — название топика становится названием индекса. `search.public.products` → `products_v1`. Регулярка `search\.public\.(.*)` плюс replacement `$1_v1` решают это в одной строке. Ради переключения версий (см. ниже) v1-суффикс зашит прямо в config — для v2 берётся отдельный коннектор с `$1_v2`.

## Index template, или почему mapping живёт в репозитории

ES умеет сам угадать типы полей по первым нескольким документам — это называется dynamic mapping. На небольшом sandbox'е работает и так, но на проде это выстреливает в ногу: первый документ пришёл с `tags: ["a","b"]`, тип определился как text → дальше документ с `tags: 42` падает с ошибкой mapping conflict. Лечится только пересозданием индекса с явным mapping'ом.

Поэтому правильно — заранее зафиксировать структуру через index template. Шаблон применяется к индексам по pattern'у имени, и любой свежесозданный `products_v2` уже стартует с нужным analyzer'ом и типами.

```json
{
  "index_patterns": ["products_*", "articles_*", "users_*"],
  "template": {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0,
      "analysis": {"analyzer": {"ru_en_text": {...}}}},
    "mappings": {"dynamic": true, "properties": {
      "name":  {"type": "text", "analyzer": "ru_en_text"},
      "price_cents": {"type": "long"}, ...
    }}
  }
}
```

Полная версия — в `es-template.json`. Анализатор тут учебный: lowercase плюс asciifolding (свернуть кириллицу-латиницу в близкие формы). На проде сюда обычно ещё подвешивают morphology-токенизатор под язык; в sandbox'е он только усложнял бы установку.

## Что показывает наш код

`cmd/db-loader/main.go` — это просто наполнитель Postgres. Тонкий wrapper над INSERT/UPDATE/DELETE с предсказуемыми значениями (id-ы числовые, name содержит слово "alpha", чтобы потом по нему делать match-query). Вот сердцевина — три INSERT'а в одной транзакции:

```go
_, err := tx.Exec(ctx, `
    INSERT INTO products (id, sku, name, description, category, price_cents, stock)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
`,
    pid,
    fmt.Sprintf("SKU-%07d", pid),
    fmt.Sprintf("Product %d %s", pid, randomWord()),
    fmt.Sprintf("Описание товара %d. Подходит для %s. Качество отличное.", pid, ...),
    categories[int(pid)%len(categories)],
    int64(100+rand.IntN(990_000)),
    rand.IntN(100),
)
```

Никакой работы с Kafka напрямую тут нет — Debezium читает WAL и публикует CDC сам. Это и есть смысл декларативного ETL: писатель Postgres'а ничего не знает про существование Elasticsearch.

`cmd/search-tester/main.go` — обратный диагностический срез. Считает строки в Postgres, документы в ES (`_count` API), делает match-query по полю и печатает топ-5 хитов. Полезен, чтобы быстро понять «дошло — не дошло».

```go
pgCount, err := countPostgres(ctx, pool, *pgTable)
esCount, err := countES(ctx, *esURL, *alias)

if pgCount != esCount {
    fmt.Printf("РАСХОЖДЕНИЕ: %d (PG) vs %d (ES)\n", pgCount, esCount)
}

hits, err := matchQuery(ctx, *esURL, *alias, *matchField, *query)
```

Если расхождение постоянное — pipeline где-то стоит. Дальше идёт `make connector-status`, и обычно видно либо FAILED-таску в Sink'е, либо что Debezium застрял на slot'е (Postgres держит replication slot до явного дропа, и если коннектор упал, slot копит WAL и в какой-то момент исчерпает диск).

## Blue-green reindex

Это та часть, ради которой паттерн «индекс с версионным суффиксом плюс alias» вообще существует. Простой случай: добавили в Postgres колонку, обновили index template, пересоздали индекс — поиск временно ничего не возвращает, пока CDC не перельёт всю таблицу заново. Не годится.

Решение: алиас `products` указывает на `products_v1`, приложение читает только через alias. Когда нужно переехать на новый mapping — поднимаем второй ES Sink, который пишет в `products_v2`, ждём, пока он догонит v1 (обычно это пара минут на сотнях тысяч строк), переключаем alias на v2 одним атомарным запросом, удаляем v1 sink. Приложение ничего не заметило.

Ключевой шаг — переключение alias. ES делает это атомарно, и если запрос пришёл за миллисекунду до и за миллисекунду после — он не видит «промежуточного» состояния. Под капотом:

```bash
curl -X POST http://localhost:19200/_aliases -d '{
  "actions": [
    {"remove": {"index": "products_v1", "alias": "products"}},
    {"add":    {"index": "products_v2", "alias": "products"}}
  ]
}'
```

Внутри одного запроса с `actions` Elasticsearch гарантирует атомарность — нет момента, когда alias не указывает ни на что. Это и есть разница с тем, чтобы делать DELETE и POST по отдельности.

Цель `make reindex-blue-green` автоматизирует весь сценарий: создаёт v2 sink, ждёт догона, переключает alias, удаляет v1 sink. Для лекции этого хватает; на проде ещё прикручивают canary-чтение из v2 заранее (через alias с `is_write_index` плюс read-only alias), чтобы не переключать вслепую.

## Как поднять

Корневой стенд (kafka-1/2/3, kafka-connect, schema-registry) должен быть запущен. Plugins для Connect (Debezium PostgresConnector + Confluent ES Sink) — установлены через `make connect-install-plugins` из корня `lectures/`. Это разовая операция, описана в Task 34.5 плана.

Дальше из директории use case'а:

```sh
make up                                 # Postgres + Elasticsearch
make pg-init                            # таблицы + publication
make es-init                            # index template
make topic-create-all                   # CDC + DLQ топики
make connect-plugin-check               # проверить plugins
make connector-create-all               # Debezium + ES Sink v1
make run-load DLOAD_COUNT=200           # залить нагрузку в Postgres
make connector-status                   # обе таски в RUNNING
make run-search                         # сравнить counts + топ-5 хитов
```

Через минуту-две `products_v1` в ES должен содержать 200 документов, search-tester — найти их по слову из поля `name`. Если расхождение — `make connector-restart` (он сбросит failed task'и) и снова `make connector-status`.

Для blue-green демо:

```sh
make reindex-blue-green                 # сам создаст alias (если не было), создаст v2, догонит, переключит
make alias-show                         # покажет, что products → products_v2
```

`reindex-blue-green` зависит от `alias-init` — тот идемпотентно создаёт `products → products_v1`. Без alias'а атомарный remove+add упадёт целиком (ES rolling back при первой неуспешной action в `_aliases`).

Снести всё:

```sh
make clean                              # удаляет коннекторы, slot, топики, контейнеры
```

## Что проверяет integration test

`test/integration_test.go` (build tag `integration`) делает то же самое, что ручной сценарий выше, плюс ещё две проверки.

- UPDATE одной строки в Postgres → отслеживание поля name в ES до изменения. Дедлайн 90 сек, обычно укладывается за 2–5 секунд (Sink linger.ms=200 + время на CDC propagation).
- DELETE одной строки → ожидание, пока документ исчезнет (`HEAD /_doc/<id>` возвращает 404). Тоже 90 сек.
- Blue-green: создаётся v2 sink с уникальным suffix'ом (чтобы не пересекаться с другими прогонами), ждём догона, переключаем alias. Проверяется, что alias реально указывает на v2.

Размер N=200 — для скорости прогона на dev-машине. Паттерн идентичен любому объёму: 50k или 500k поведут себя так же, изменятся только цифры в дедлайнах. Прогон теста занимает примерно 1–2 минуты, основное время — стартап двух Connect-коннекторов и ожидание snapshot phase Debezium'а.

```sh
make up && make pg-init && make es-init && make test-integration
```

Тест сам делает truncate Postgres, удаляет старые `products_v*` индексы, дропает старые replication slot'ы (`usecase_09_04_it_%`) — между прогонами руками чистить не надо.

## Файлы

```
04-pg-to-elasticsearch/
├── README.md                          ← этот файл
├── Makefile                           ← цели up/down/connector-*/test-integration/reindex-blue-green
├── docker-compose.override.yml        ← Postgres (15443) + ES (19200)
├── go.mod                             ← зависимости (pgx + franz-go для теста)
├── es-template.json                   ← index template (settings + mappings)
├── connectors/
│   ├── debezium-pg-source.json        ← Debezium PostgresConnector
│   ├── es-sink.json                   ← ES Sink, route → *_v1
│   └── es-sink-v2.json                ← то же, route → *_v2 (для blue-green)
├── db/
│   └── init.sql                       ← таблицы products/articles/users + publication
├── cmd/
│   ├── db-loader/main.go              ← INSERT/UPDATE/DELETE в Postgres
│   └── search-tester/main.go          ← диагностика: PG count vs ES count + match-query
└── test/
    └── integration_test.go            ← E2E с blue-green reindex
```

## Что осталось за кадром

Sandbox-вариант намеренно урезан. На проде ту же схему обычно дополняют так.

Авторизация ES — здесь `xpack.security.enabled: "false"`, потому что одна команда учится паттерну, а не настройке security. В реальном кластере Sink работает через `connection.username`/`connection.password` или API key. Конфиг ES Sink принимает оба варианта без изменений в SMT-цепочке.

Schema Registry — здесь Debezium и Sink говорят через JsonConverter. Это удобно для дебага (открыл консоль и читаешь). Под нагрузку JSON неэффективен, и обычно цепочку переводят на Avro через SR — оба коннектора это поддерживают, надо только заменить пары `*.converter` и поднять SR (он уже есть в корневом стенде, лекция [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md) про него отдельно).

Multi-tenant индексы — в этой лекции `products_v1` глобальный. Если каталог режется по магазинам/тенантам, обычно делают шаблон `products_<tenant>_v1` через тот же RegexRouter, плюс расширенный template. Логика остаётся.

Реактивный canary при reindex'е — тут переключение alias делается в одно действие. На проде сначала alias с `is_write_index=true` для v2 и read-only-alias на v1, какое-то время оба индекса живут, и читать можно с постепенной миграцией трафика. Реализуется тем же `_aliases` API, только с большим количеством actions.

ETL backfill из source-of-truth, отличного от текущего Postgres — тут Debezium со `snapshot.mode=initial` снимает таблицу целиком при первом запуске. Если данные приходят откуда-то ещё (S3-snapshot, dump из старой БД), backfill делают отдельным процессом, обычно через bulk-индексирование напрямую в ES, и только новые изменения идут через CDC. Паттерн: «historical bulk + live CDC» — стандартный для миграций под поиск.
