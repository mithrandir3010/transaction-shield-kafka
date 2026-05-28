-- UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enum: transaction lifecycle states
CREATE TYPE transaction_status AS ENUM ('PENDING', 'SCORED', 'APPROVED', 'FLAGGED', 'REJECTED');

-- ── transactions ────────────────────────────────────────────────────
CREATE TABLE transactions (
    id                 UUID              PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id     VARCHAR(64)       NOT NULL UNIQUE,
    account_id         VARCHAR(64)       NOT NULL,
    merchant_id        VARCHAR(64)       NOT NULL,
    amount             NUMERIC(19, 4)    NOT NULL,
    currency           CHAR(3)           NOT NULL DEFAULT 'USD',
    status             transaction_status NOT NULL DEFAULT 'PENDING',
    fraud_score        SMALLINT,
    ip_address         INET,
    device_fingerprint VARCHAR(128),
    country_code       CHAR(2),
    created_at         TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    scored_at          TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ       NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_account_id ON transactions (account_id);
CREATE INDEX idx_transactions_status     ON transactions (status);
CREATE INDEX idx_transactions_created_at ON transactions (created_at DESC);

-- ── fraud_rules ─────────────────────────────────────────────────────
CREATE TABLE fraud_rules (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_code    VARCHAR(64) NOT NULL UNIQUE,
    description  TEXT,
    score_weight SMALLINT    NOT NULL DEFAULT 10,
    enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── idempotency_log ─────────────────────────────────────────────────
-- Durable fallback for Redis idempotency keys
CREATE TABLE idempotency_log (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    event_type      VARCHAR(64)  NOT NULL,
    payload_hash    VARCHAR(64),
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_idempotency_log_expires_at ON idempotency_log (expires_at);

-- ── Trigger: auto-update updated_at ─────────────────────────────────
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

CREATE TRIGGER trg_fraud_rules_updated_at
    BEFORE UPDATE ON fraud_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
