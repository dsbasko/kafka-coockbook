-- Таблица, в которую JDBC sink connector будет автоматически апсёртить заказы.
-- Имя поля и типы должны соответствовать schema из JSON-сообщений (см. cmd/orders-producer).
-- pk.mode=record_value + pk.fields=id у sink'а делает id первичным ключом —
-- значит INSERT с уже существующим id превращается в UPSERT, а не валится с
-- duplicate key.
CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT       PRIMARY KEY,
    customer_id TEXT         NOT NULL,
    amount      DOUBLE PRECISION NOT NULL,
    status      TEXT         NOT NULL,
    created_at  TEXT         NOT NULL
);
