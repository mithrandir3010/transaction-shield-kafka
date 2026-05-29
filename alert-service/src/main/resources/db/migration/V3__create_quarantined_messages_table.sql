-- ── quarantined_messages ────────────────────────────────────────────────
-- Stores Kafka DLQ messages that cannot be automatically recovered:
--   FATAL        — deserialization / schema mismatch, payload is unreadable
--   TRANSIENT    — exceeded max replay attempt threshold (default 3)
--
-- Lifecycle:  OPEN → RESOLVED | DISCARDED  (via operator action)
-- Unique key: (transaction_id, original_topic) — prevents duplicate quarantine records
--             from concurrent QuarantineConsumer + DlqReplayService inserts.

CREATE TABLE quarantined_messages (
    id                UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id    VARCHAR(64)  NOT NULL,
    original_topic    VARCHAR(128) NOT NULL,
    error_category    VARCHAR(32)  NOT NULL,
    exception_class   VARCHAR(256),
    exception_message TEXT,
    replay_attempts   INTEGER      NOT NULL DEFAULT 0,
    status            VARCHAR(32)  NOT NULL DEFAULT 'OPEN',
    quarantined_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at       TIMESTAMPTZ,
    resolved_by       VARCHAR(128),
    resolution_notes  TEXT,

    CONSTRAINT uq_quarantine_txid_topic UNIQUE (transaction_id, original_topic)
);

CREATE INDEX idx_quarantine_status      ON quarantined_messages (status);
CREATE INDEX idx_quarantine_category    ON quarantined_messages (error_category);
CREATE INDEX idx_quarantine_quarantined ON quarantined_messages (quarantined_at DESC);
