-- Write-side. order-service пишет orders + outbox в одной транзакции.
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    currency     TEXT         NOT NULL,
    status       TEXT         NOT NULL DEFAULT 'NEW',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Outbox в той же БД, что orders. Транзакция атомарна на уровне Postgres,
-- никакой Kafka внутри write-path не зовём — её зовёт outbox publisher.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL    PRIMARY KEY,
    aggregate_id  TEXT         NOT NULL,
    topic         TEXT         NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ  NULL
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- Read-side. orders_view обновляется отдельным consumer'ом из Kafka.
-- Это точка асинхронного eventual consistency: write-path → kafka → projector → view.
-- Между Create и тем, как Get отдаст этот id, есть лаг. Чем больше нагрузка
-- и хуже сеть — тем больше лаг. Это не баг, это контракт CQRS.
CREATE TABLE IF NOT EXISTS orders_view (
    id           TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    currency     TEXT         NOT NULL,
    status       TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    projected_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Локальное состояние inventory-сервиса. Он живёт «как будто отдельно»,
-- но в учебном sandbox мы шарим один Postgres. В проде это была бы
-- собственная БД сервиса.
CREATE TABLE IF NOT EXISTS inventory_reservations (
    order_id     TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    reserved_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Dedup для проектора и для inventory: outbox-id из header'а Kafka-сообщения.
-- INSERT ON CONFLICT DO NOTHING + RowsAffected проверка → at-least-once
-- становится effectively-once на уровне БД-стейта.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     TEXT         NOT NULL,
    outbox_id    BIGINT       NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, outbox_id)
);
