-- uuid-ossp may already exist (fraud-engine migration runs first on shared DB)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── alerts ──────────────────────────────────────────────────────────
-- alert-service owns this table. transaction_id has no FK to transactions
-- so alert-service can persist independently of fraud-engine.
-- UNIQUE constraint on transaction_id provides DB-level idempotency.
CREATE TABLE alerts (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id  VARCHAR(64) NOT NULL UNIQUE,
    user_id         VARCHAR(64) NOT NULL,
    fraud_score     SMALLINT    NOT NULL,
    risk_level      VARCHAR(20) NOT NULL,
    triggered_rules VARCHAR(512),
    status          VARCHAR(30) NOT NULL DEFAULT 'OPEN',
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_transaction_id ON alerts (transaction_id);
CREATE INDEX idx_alerts_status         ON alerts (status);
CREATE INDEX idx_alerts_risk_level     ON alerts (risk_level);
CREATE INDEX idx_alerts_created_at     ON alerts (created_at DESC);

-- CREATE OR REPLACE is idempotent — safe if fraud-engine already defined this function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_alerts_updated_at
    BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
