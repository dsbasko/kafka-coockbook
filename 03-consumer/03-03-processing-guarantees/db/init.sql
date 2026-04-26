CREATE TABLE IF NOT EXISTS messages (
    topic        TEXT        NOT NULL,
    partition    INTEGER     NOT NULL,
    "offset"     BIGINT      NOT NULL,
    payload      TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition, "offset")
);
