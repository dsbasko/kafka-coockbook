-- ReplacingMergeTree для аналитических таблиц: при дублях по PK ClickHouse
-- оставит запись с максимальным значением version-колонки. Это решает две
-- проблемы CDC-стриминга:
--   1) at-least-once Debezium → возможные дубликаты после восстановления
--      коннектора с replication slot'а;
--   2) UPDATE в Postgres приходит как новое событие — мы версионируем
--      по cdc_lsn (логический log sequence number), который монотонно растёт.
--
-- При SELECT с FINAL ClickHouse возвращает уже схлопнутую версию.
-- Для аналитических ad-hoc-запросов FINAL дороговат, поэтому в проде
-- обычно ставят OPTIMIZE TABLE ... FINAL по расписанию или используют
-- агрегатные движки. В учебном sandbox — FINAL руками в тесте.
--
-- _deleted = 1 ставим, когда из Postgres пришла операция d (DELETE):
-- ReplacingMergeTree сам по себе не умеет hard delete, но фильтр
-- по _deleted=0 даёт логическое удаление в SELECT'ах.
--
-- Имена колонок намеренно совпадают с тем, что выдаёт anonymizer (правило
-- hash меняет значение поля, но не имя): email, phone, full_name. Под
-- хэшем подразумевается SHA-256-hex, что валидно как обычный String.
--
-- created_at/updated_at — String, потому что Debezium для TIMESTAMPTZ шлёт
-- ISO-строку, и ClickHouse JSONEachRow без `date_time_input_format=best_effort`
-- падает на ней с code 27. Хранить в String — значит платить ~30 байт за
-- timestamp вместо 8, но в учебном sandbox наглядность важнее.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.users (
    id          Int64,
    email       String,                -- SHA-256-hex от исходного email
    phone       String,                -- SHA-256-hex от phone
    full_name   String,                -- truncated до "first" + initial + "."
    country     String,
    created_at  String,
    updated_at  String,
    cdc_lsn     UInt64,
    _deleted    UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_lsn)
PRIMARY KEY (id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS analytics.orders (
    id           Int64,
    user_id      Int64,
    amount_cents Int64,
    currency     String,
    status       String,
    -- notes дропнут анонимизатором — хранить нечего
    created_at   String,
    cdc_lsn      UInt64,
    _deleted     UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_lsn)
PRIMARY KEY (id)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS analytics.events (
    id           Int64,
    user_id      Int64,
    event_type   String,
    -- ip_address и user_agent дропнуты анонимизатором: хранение IP-адресов
    -- неоплачиваемых событий — зона риска по GDPR/152-ФЗ. Если нужны для
    -- защиты от фрода, оставляются с маской до /16.
    created_at   String,
    cdc_lsn      UInt64,
    _deleted     UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_lsn)
PRIMARY KEY (id)
ORDER BY (id);
