-- Источник CDC: три таблицы с PII-полями. Анонимизатор маскирует email/phone,
-- truncate полные имена, дропает birth_date, остальное оставляет как есть.
-- REPLICA IDENTITY FULL нужен Debezium'у для UPDATE/DELETE: без него поле
-- before в событии содержит только PK, и анонимизатор не видит, что было до
-- изменения — а у нас по before/after строится дельта в ClickHouse.

CREATE TABLE IF NOT EXISTS users (
    id          BIGINT       PRIMARY KEY,
    email       TEXT         NOT NULL,
    phone       TEXT         NOT NULL,
    full_name   TEXT         NOT NULL,
    birth_date  DATE         NULL,
    country     TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT       PRIMARY KEY,
    user_id     BIGINT       NOT NULL,
    amount_cents BIGINT      NOT NULL,
    currency    TEXT         NOT NULL,
    status      TEXT         NOT NULL,
    notes       TEXT         NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS events (
    id          BIGINT       PRIMARY KEY,
    user_id     BIGINT       NOT NULL,
    event_type  TEXT         NOT NULL,
    ip_address  TEXT         NULL,
    user_agent  TEXT         NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE events REPLICA IDENTITY FULL;

-- Publication заранее: даём Debezium'у явный список таблиц вместо
-- publication.autocreate.mode=filtered. Так видно, что именно стримится,
-- и можно править список без удаления коннектора.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR TABLE users, orders, events;
    END IF;
END $$;
