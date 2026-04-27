CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL    PRIMARY KEY,
    customer_id TEXT         NOT NULL,
    amount      NUMERIC      NOT NULL,
    status      TEXT         NOT NULL DEFAULT 'created',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL    PRIMARY KEY,
    aggregate_id  TEXT         NOT NULL,
    topic         TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ  NULL
);

-- Partial index по неопубликованным — основной запрос publisher'а летит по нему.
-- На пусто → быстрый seq scan; на разогретой outbox с миллионом записей — index scan по короткому списку.
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- Dedup-таблица на стороне consumer'а. PRIMARY KEY (outbox_id) +
-- INSERT ON CONFLICT DO NOTHING поглощает дубли, прилетевшие после
-- crash'а publisher'а между produce и UPDATE.
CREATE TABLE IF NOT EXISTS processed_outbox_ids (
    outbox_id     BIGINT       PRIMARY KEY,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
