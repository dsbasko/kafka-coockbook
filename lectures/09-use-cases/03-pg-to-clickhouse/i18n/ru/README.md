# 09-03 — Postgres → ClickHouse с анонимизацией

ETL для аналитики. Слева Postgres с реальными PII, справа ClickHouse, в который нельзя класть сырые персональные данные. Между ними — Debezium (snapshot + live CDC), наш Go-анонимизатор и ClickHouse Sink connector. Use case собирает CDC из [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/ru/README.md), контракты из [Protobuf в Go](../../../../05-contracts/05-02-protobuf-in-go/i18n/ru/README.md), паттерн декларативной маски и ReplacingMergeTree для дедупликации.

## Что собираем

```
Postgres (15442)
   │
   │  WAL (logical replication, slot)
   ▼
kafka-connect: Debezium PostgresConnector
   │
   ▼
cdc.public.users / cdc.public.orders / cdc.public.events
   │
   ▼
anonymizer (Go, consumer-group)
   │  применяет anonymize.yaml
   ▼
analytics.users / analytics.orders / analytics.events
   │
   ▼
kafka-connect: ClickHouse Sink
   │
   ▼
ClickHouse (18123) → ReplacingMergeTree(cdc_lsn)
```

Главное отличие от [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/ru/README.md) — два звена вместо одного. В [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/ru/README.md) Debezium гнал чистый CDC прямо потребителю, тут между Debezium и Sink'ом стоит анонимизатор: он читает Debezium-события, обрезает PII по правилам YAML и отдаёт в `analytics.*` уже безопасную версию. ClickHouse Sink этих правил не знает и ему не нужно — он просто пишет JSON в таблицы.

## Зачем посредник вместо Connect SMT

Можно было бы сделать всё на Single Message Transforms внутри ClickHouse Sink. Так делать не стали:

- правила нужны разные для разных таблиц (где-то hash, где-то drop, где-то truncate);
- truncate имени по локали — это уже логика, а не подстановка регуляркой;
- соль для hash хочется хранить отдельно от Connect-конфига и не вписывать в JSON-конфиг коннектора как plain text;
- ошибки маски через SMT тяжело тестировать — у Connect нет нормального unit-test обвязки;
- декларативный YAML с правилами читают и редактируют не разработчики.

Поэтому правила вынесены в отдельный Go-сервис с табличными unit-тестами (см. `internal/anonymizer/rules_test.go`). Connect занят тем, что у него получается: чтение WAL и пакетная вставка в ClickHouse.

## Декларативный конфиг масок

`anonymize.yaml` описывает, что делать с полями. Поддерживаются четыре правила: hash, drop, truncate и passthrough (он же дефолт).

```yaml
salt: "usecase-09-03-salt"

tables:
  users:
    target_topic: analytics.users
    fields:
      email:      hash
      phone:      hash
      full_name:  truncate
      birth_date: drop      # сырая дата рождения — PII

  orders:
    target_topic: analytics.orders
    fields:
      notes: drop           # свободный текст пользователя
```

Поле `id` или `created_at` явно перечислять не надо — без правила сработает passthrough. Это чтобы YAML не пух пустыми строчками.

`hash` — это SHA-256 от соли плюс значения, в hex. Стабильный (один и тот же email с одной солью даёт один и тот же hash во всех окружениях, можно делать join по email_hash в аналитике). Соль в репо — потому что sandbox; в проде она в секрете и в YAML ссылка через `${ANONYMIZE_SALT}` или подобное.

`truncate` — формат `{первое слово}{инициал второго}.`. «Иван Иванов» становится «ИванИ.». Полезно, когда хочется хранить категорию low-cardinality имени без раскрытия.

`drop` — поле просто не попадает в выходной payload. Колонки в ClickHouse-таблице тоже нет, так что Sink не даст случайно вспомнить о нём.

## Что показывает наш код

Главное — функция `transform()` в `internal/anonymizer/runtime.go`. Она берёт Debezium-record и решает, что отдать в `analytics.*`:

```go
switch env.Op {
case "d":
    raw = env.Before
    deleted = 1
case "c", "u", "r":
    raw = env.After
default:
    return nil, nil
}
```

DELETE превращается в строку с `_deleted=1` (взяли поле `before`, потому что после удаления `after` пустой). INSERT, UPDATE и REPLAY snapshot'а (`r`) — берут `after` с `_deleted=0`. Truncate-операции и неизвестные op'ы пропускаются — нам их в аналитике не надо.

Дальше — применение правил из YAML:

```go
cleaned, ok := cfg.Apply(table, raw)
if !ok {
    return nil, nil
}

cleaned["cdc_lsn"] = lsnFromSource(env.Source)
cleaned["_deleted"] = deleted
```

`Apply` — это `internal/anonymizer/rules.go`. Switch по правилу для каждого поля, неизвестное поле копируется как passthrough:

```go
switch rule {
case RuleDrop:
    continue
case RuleHash:
    out[k] = hashWithSalt(c.Salt, v)
case RuleTruncate:
    out[k] = truncateName(v)
case RulePassthrough:
    out[k] = v
}
```

`cdc_lsn` берём из поля `source.lsn` Debezium-события. Это монотонно растущий 64-битный счётчик из Postgres'а, идеальный version-column для ReplacingMergeTree. Если LSN внезапно нет (битый event), fallback на `ts_ms` — для аналитики достаточно.

### Главный цикл

`Run` — обычный poll-loop:

```go
fetches := cl.PollFetches(ctx)
...
fetches.EachRecord(func(r *kgo.Record) {
    rec, err := transform(r, opts.Config)
    ...
    outRecords = append(outRecords, rec)
})
if len(outRecords) > 0 {
    res := prod.ProduceSync(ctx, outRecords...)
    ...
}
if err := cl.CommitUncommittedOffsets(ctx); err != nil {
    return fmt.Errorf("commit offsets: %w", err)
}
```

At-least-once. Между `ProduceSync` и `CommitUncommittedOffsets` есть окно: если процесс умрёт, batch уйдёт повторно. Дубль на стороне ClickHouse схлопнется через ReplacingMergeTree — там же лежит cdc_lsn.

## ReplacingMergeTree и cdc_lsn

В `ch/init.sql`:

```sql
CREATE TABLE IF NOT EXISTS analytics.users (
    id          Int64,
    email       String,
    phone       String,
    full_name   String,
    country     String,
    created_at  String,
    updated_at  String,
    cdc_lsn     UInt64,
    _deleted    UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_lsn)
PRIMARY KEY (id)
ORDER BY (id);
```

`ReplacingMergeTree(cdc_lsn)` означает: при одинаковых ключах ORDER BY ClickHouse оставит запись с максимальным `cdc_lsn`. Свёртка происходит в фоновых merge'ах, а в ad-hoc-запросах можно дописать `FINAL`, чтобы получить уже схлопнутую версию (дороже, зато корректно).

`_deleted` — это soft-delete. ReplacingMergeTree не умеет hard-delete, и нам он не нужен: по `WHERE _deleted=0` отфильтровываем «удалённые», для GDPR-аудита всё равно полезно видеть событие.

Время хранится как String. Это компромисс: Debezium для TIMESTAMPTZ шлёт ISO-строку, а JSONEachRow без `date_time_input_format=best_effort` падает на ней с code 27. Можно было бы добавить настройку Sink'у, но в учебном sandbox String проще — двойного конфигурирования не надо. В проде платят 20–30 байт за timestamp вместо 8 и идут дальше.

## REPLICA IDENTITY FULL

В `db/init.sql`:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

Без этого Postgres в WAL для UPDATE/DELETE кладёт только PK в поле `before`. Анонимизатору это безразлично для UPDATE (мы смотрим только `after`), но для DELETE — критично: на soft-delete мы хотим знать, какой email был у удалённого юзера, чтобы дедуп по email_hash совпал. С `REPLICA IDENTITY FULL` Debezium шлёт всю строку до изменения. Цена — больше WAL'а; на учебной нагрузке незаметно.

## Дубли: где появляются и где гасятся

Источников дубля два:

1. Восстановление anonymizer'а после краша. Между produce в `analytics.*` и commit'ом offset'а есть окно. Перезапуск читает batch заново.
2. Replication slot Debezium'а после рестарта kafka-connect. Connector помнит LSN, до которого он закоммитил в Kafka, но между WAL-чтением и Kafka-записью тоже есть at-least-once окно.

Оба гасятся одинаково: в ClickHouse дубли отличаются от оригинала только тем, что `cdc_lsn` совпадает (или у дубля он чуть младше при ретрае). ReplacingMergeTree сольёт их в одну версию. Если хочется паранойи — `OPTIMIZE TABLE ... FINAL` по расписанию.

## Snapshot + live стрим

В `connectors/debezium-pg-source.json`:

```json
"snapshot.mode": "initial",
"slot.name": "usecase_09_03_slot",
"publication.name": "dbz_publication"
```

Connector при первом запуске делает initial-snapshot: читает все строки из `users/orders/events` и шлёт их в `cdc.public.*` как op=`r` (read). После snapshot'а переключается на чтение WAL через replication slot — каждое INSERT/UPDATE/DELETE приходит с op=`c`, `u`, `d`.

Анонимизатору всё равно, snapshot это или live: он одинаково обрабатывает `c`, `u`, `r` (берёт `after`), отдельно — `d` (берёт `before`, ставит `_deleted=1`). Backfill получается бесплатно: подсунули новую таблицу, перезапустили connector со свежим slot'ом, snapshot прокатил всю историю.

## Integration test

`test/integration_test.go` (build tag `integration`) — полный E2E. Запускает поток:

1. Чистит Postgres, ClickHouse, Kafka-топики, consumer-группы.
2. Регистрирует Debezium и ClickHouse Sink через REST API Connect'а с уникальным slot-именем (нужно, чтобы прошлый запуск с дропнутой Postgres-инстанцией не оставил sticky offset в `_connect-offsets`).
3. Запускает anonymizer в горутине.
4. Льёт 100 строк в Postgres, ждёт пока пройдёт snapshot.
5. Убивает anonymizer, льёт ещё 100 строк, поднимает заново — проверяет recovery с committed offset'а.
6. Ждёт появления `>= 200` строк в каждой ClickHouse-таблице с дедлайном 4 минуты на users (там snapshot самый длинный).
7. Проверяет три инварианта анонимизации: email/phone — sha256-hex, full_name заканчивается на точку, в events есть запись с `_deleted=1`.

Тест требует поднятого корневого стенда, `make up && make pg-init && make ch-init && make connect-install-plugins` и плагинов в `connect-plugins/`. Без них автоматически skip — `t.Skipf` в начале с понятным сообщением.

Дополнительно есть `test/anonymizer_test.go` без Connect'а: льёт прямо в `cdc.public.*` JSON в формате Debezium, проверяет, что anonymizer корректно отдаёт в `analytics.*`. Полезен, когда Kafka есть, а Connect ставить лень.

## Файлы

- `anonymize.yaml` — декларативные правила.
- `db/init.sql` — таблицы Postgres + publication для Debezium.
- `ch/init.sql` — analytics-таблицы на ReplacingMergeTree.
- `connectors/debezium-pg-source.json` — конфиг source-коннектора.
- `connectors/clickhouse-sink.json` — конфиг sink-коннектора.
- `proto/analytics/v1/events.proto` — контракты clean-топиков (на wire JSON, proto-файл документирует структуру).
- `cmd/anonymizer/main.go` — тонкая CLI-обёртка над `internal/anonymizer.Run`.
- `cmd/db-loader/main.go` — генератор INSERT/UPDATE/DELETE для ручных прогонов.
- `internal/anonymizer/runtime.go` — `Run` и `transform`.
- `internal/anonymizer/rules.go` — `Apply`, `hashWithSalt`, `truncateName`.
- `test/integration_test.go` — полный E2E с Debezium + ClickHouse.
- `test/anonymizer_test.go` — pipeline-component тест без Connect.
- `docker-compose.override.yml` — Postgres 15442 + ClickHouse 18123, обе с healthcheck.

## Запуск

Корневой стенд должен быть поднят (`docker compose up -d` из корня), плагины Connect — установлены (`make -C lectures connect-install-plugins`).

```bash
make up                          # Postgres + ClickHouse
make pg-init                     # таблицы + publication
make ch-init                     # analytics-схемы
make topic-create-all            # cdc.public.* и analytics.*
make connect-plugin-check        # проверить, что Debezium и CH Sink доступны
make connector-create-all        # зарегистрировать оба коннектора
make connector-status            # должны быть в RUNNING

# в отдельном терминале:
make run-anonymizer

# в третьем — нагрузка:
make db-load DLOAD_COUNT=200
```

Через десяток секунд в ClickHouse появятся данные:

```bash
make ch-shell
> SELECT count() FROM analytics.users;
> SELECT email FROM analytics.users LIMIT 3;   -- увидим sha256-hex
> SELECT full_name FROM analytics.users LIMIT 3; -- увидим "User1L." и т.п.
```

Когда наигрались — `make clean` дропнет коннекторы, replication-slot'ы и контейнеры. Volume для Postgres и ClickHouse тоже удалится: следующий запуск стартует с пустого состояния.

## Что осталось за кадром

- Schema Registry и `sr.Serde`. ClickHouse Sink по дефолту читает JsonConverter, для Avro/Protobuf через SR нужна другая комбинация конвертеров. Задача про SR-обёртку решена в [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md); если хочется добавить — поменять `value.converter` в обоих коннекторах и обернуть `prod.ProduceSync` в `sr.Serde`.
- Tombstones: `tombstones.on.delete: true` в Debezium шлёт пустой record после `d`. Anonymizer его пропускает (см. `transform`, `r.Value == nil`), потому что `_deleted=1` уже отправлен с before-кадра. Если Sink захочет tombstone для compaction — поменять логику.
- Бэкап и replay: после реальной аварии Connect может ругаться на «replication slot does not exist». Лечится `pg_drop_replication_slot` (в Makefile цель `connector-delete-all` это уже делает) и пересозданием коннектора с `snapshot.mode: initial`.
- Производительность: при больших объёмах (миллионы строк) ClickHouse Sink начинает упираться в batch settings. Полезные ручки — `clickhouse.bulk.size`, `consumer.override.fetch.min.bytes`. В sandbox их не трогаем.
