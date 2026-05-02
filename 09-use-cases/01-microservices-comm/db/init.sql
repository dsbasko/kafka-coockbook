-- order-service: write-side. orders + outbox в одной транзакции.
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    currency     TEXT         NOT NULL,
    status       TEXT         NOT NULL DEFAULT 'NEW',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL    PRIMARY KEY,
    aggregate_id  TEXT         NOT NULL,
    topic         TEXT         NOT NULL,
    payload       BYTEA        NOT NULL,
    headers       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ  NULL
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- inventory-service: локальное состояние.
CREATE TABLE IF NOT EXISTS inventory_reservations (
    order_id     TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    reserved_by  TEXT         NOT NULL,
    reserved_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- notification-service: история отправленных уведомлений (mock — без реального канала).
CREATE TABLE IF NOT EXISTS notifications_log (
    order_id     TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    channel      TEXT         NOT NULL,
    sent_by      TEXT         NOT NULL,
    sent_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Dedup для всех consumer'ов: PRIMARY KEY (consumer, outbox_id)
-- INSERT ON CONFLICT DO NOTHING + RowsAffected = effectively-once.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     TEXT         NOT NULL,
    outbox_id    BIGINT       NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, outbox_id)
);
