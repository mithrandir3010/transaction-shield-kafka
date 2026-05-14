-- Transaction Shield — PostgreSQL Initialization
-- Runs once on first container startup

-- UUID extension for primary keys
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enum types
CREATE TYPE transaction_status AS ENUM ('PENDING', 'SCORED', 'APPROVED', 'FLAGGED', 'REJECTED');
CREATE TYPE alert_severity    AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');
CREATE TYPE alert_status      AS ENUM ('OPEN', 'INVESTIGATING', 'RESOLVED', 'FALSE_POSITIVE');

-- ── transactions ────────────────────────────────────────────────────
CREATE TABLE transactions (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id    VARCHAR(64)      NOT NULL UNIQUE,   -- idempotency key
    account_id        VARCHAR(64)      NOT NULL,
    merchant_id       VARCHAR(64)      NOT NULL,
    amount            NUMERIC(19, 4)   NOT NULL,
    currency          CHAR(3)          NOT NULL DEFAULT 'USD',
    status            transaction_status NOT NULL DEFAULT 'PENDING',
    fraud_score       SMALLINT,                           -- 0–100
    ip_address        INET,
    device_fingerprint VARCHAR(128),
    country_code      CHAR(2),
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    scored_at         TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_account_id  ON transactions (account_id);
CREATE INDEX idx_transactions_status      ON transactions (status);
CREATE INDEX idx_transactions_created_at  ON transactions (created_at DESC);

-- ── fraud_rules ─────────────────────────────────────────────────────
CREATE TABLE fraud_rules (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_code    VARCHAR(64)  NOT NULL UNIQUE,  -- e.g. HIGH_AMOUNT, VELOCITY_CHECK
    description  TEXT,
    score_weight SMALLINT     NOT NULL DEFAULT 10,
    enabled      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed default rules
INSERT INTO fraud_rules (rule_code, description, score_weight, enabled) VALUES
    ('HIGH_AMOUNT',         'Transaction amount exceeds threshold',             30, TRUE),
    ('VELOCITY_CHECK',      'Too many transactions in a short time window',     25, TRUE),
    ('BLACKLISTED_COUNTRY', 'Transaction originates from a blacklisted country',40, TRUE),
    ('SUSPICIOUS_HOUR',     'Transaction at unusual hours (02:00–05:00)',        15, TRUE),
    ('NEW_DEVICE',          'First-time device fingerprint for this account',   10, TRUE);

-- ── alerts ──────────────────────────────────────────────────────────
CREATE TABLE alerts (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID         NOT NULL REFERENCES transactions(id),
    severity       alert_severity NOT NULL,
    status         alert_status   NOT NULL DEFAULT 'OPEN',
    triggered_rules VARCHAR(512),              -- comma-separated rule codes
    fraud_score    SMALLINT     NOT NULL,
    notes          TEXT,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_transaction_id ON alerts (transaction_id);
CREATE INDEX idx_alerts_status         ON alerts (status);
CREATE INDEX idx_alerts_severity       ON alerts (severity);

-- ── idempotency_log ─────────────────────────────────────────────────
-- Backup store: Redis is primary; Postgres is the durable fallback
CREATE TABLE idempotency_log (
    idempotency_key  VARCHAR(128) PRIMARY KEY,
    event_type       VARCHAR(64)  NOT NULL,
    payload_hash     VARCHAR(64),             -- SHA-256 of original payload
    processed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at       TIMESTAMPTZ  NOT NULL    -- mirrors Redis TTL
);

CREATE INDEX idx_idempotency_log_expires_at ON idempotency_log (expires_at);

-- Auto-update updated_at via trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transactions_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_alerts_updated_at
    BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_fraud_rules_updated_at
    BEFORE UPDATE ON fraud_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
