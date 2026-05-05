-- notifications_log: история отправленных уведомлений по каждому каналу.
-- status: 'delivered' (успех HTTP), 'dlq' (исчерпали retry-ступени).
CREATE TABLE IF NOT EXISTS notifications_log (
    notification_id TEXT         NOT NULL,
    channel         TEXT         NOT NULL,
    user_id         TEXT         NOT NULL,
    status          TEXT         NOT NULL,
    attempts        INT          NOT NULL DEFAULT 1,
    sent_by         TEXT         NOT NULL,
    last_error      TEXT         NOT NULL DEFAULT '',
    sent_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (notification_id, channel)
);

CREATE INDEX IF NOT EXISTS notifications_log_channel_status_idx
    ON notifications_log (channel, status);

-- Дедуп для всех consumer'ов: PRIMARY KEY (consumer, notification_id).
-- INSERT ON CONFLICT DO NOTHING + RowsAffected = effectively-once.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer        TEXT         NOT NULL,
    notification_id TEXT         NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer, notification_id)
);
