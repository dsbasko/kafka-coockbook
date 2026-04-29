-- saga_state живёт только в orchestration-варианте. Это центральная
-- ленточная диаграмма саги: один ряд на одну запущенную сагу,
-- current_step показывает, на каком шаге она сейчас, status — итог.
--
-- В choreography такой таблицы нет вообще: состояние размазано между
-- сервисами, никто не видит «всю сагу» в одном месте. В этом и разница.
CREATE TABLE IF NOT EXISTS saga_state (
    saga_id      TEXT         PRIMARY KEY,
    customer_id  TEXT         NOT NULL,
    amount_cents BIGINT       NOT NULL,
    currency     TEXT         NOT NULL,
    -- PAYMENT, INVENTORY, SHIPMENT, COMPENSATING, DONE
    current_step TEXT         NOT NULL,
    -- RUNNING, SUCCESS, FAILED
    status       TEXT         NOT NULL,
    last_event   TEXT,
    payment_id   TEXT,
    reservation_id TEXT,
    shipment_id  TEXT,
    failure_reason TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS saga_state_status_idx ON saga_state (status);
