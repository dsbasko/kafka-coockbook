-- Источник CDC: три таблицы, которые поедут в Elasticsearch как индексы.
-- products — товарный каталог (full-text search), articles — статьи
-- (поиск по title + body), users — справочник для join'ов (Sink не делает
-- join, мы его не делаем — это место для отдельной search-experience-
-- лекции). REPLICA IDENTITY FULL нужен Debezium'у для UPDATE/DELETE,
-- иначе before-payload обрезается до PK и Sink не видит изменения.

CREATE TABLE IF NOT EXISTS products (
    id          BIGINT       PRIMARY KEY,
    sku         TEXT         NOT NULL,
    name        TEXT         NOT NULL,
    description TEXT         NOT NULL,
    category    TEXT         NOT NULL,
    price_cents BIGINT       NOT NULL,
    stock       INT          NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS articles (
    id          BIGINT       PRIMARY KEY,
    title       TEXT         NOT NULL,
    body        TEXT         NOT NULL,
    author      TEXT         NOT NULL,
    tags        TEXT         NOT NULL DEFAULT '',
    published_at TIMESTAMPTZ NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE articles REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS users (
    id          BIGINT       PRIMARY KEY,
    username    TEXT         NOT NULL,
    full_name   TEXT         NOT NULL,
    bio         TEXT         NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE users REPLICA IDENTITY FULL;

-- Publication заранее: явный список таблиц вместо publication.autocreate.
-- Так видно, что именно стримится. Удалить таблицу из publication
-- = `ALTER PUBLICATION dbz_publication DROP TABLE <name>`.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR TABLE products, articles, users;
    END IF;
END $$;
