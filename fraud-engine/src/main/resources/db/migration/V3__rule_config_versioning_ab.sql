-- ── Rule Engine Configurability Extensions ──────────────────────────────────
--
-- (1) fraud_rules — add variant + parameters
--     Each rule now belongs to a named variant (STABLE = production).
--     Experimental configs live under EXPERIMENT variant.
--     parameters JSONB stores rule-specific thresholds so they can be changed
--     from the DB without touching application.yml or restarting the service.
--
-- (2) scoring_audit_log — immutable record of every scoring decision
--     Captures transactionId + variant used + result so analysts can
--     compare STABLE vs EXPERIMENT outcomes for the same transaction.
--
-- (3) rule_set_experiments — controls the active A/B traffic split
--     Only one experiment may be active at a time (enforced by application).

-- ── (1) Extend fraud_rules ───────────────────────────────────────────────────

ALTER TABLE fraud_rules
    ADD COLUMN variant    VARCHAR(32) NOT NULL DEFAULT 'STABLE',
    ADD COLUMN parameters JSONB;

-- Replace single-column unique key with composite (rule_code, variant)
ALTER TABLE fraud_rules DROP CONSTRAINT fraud_rules_rule_code_key;
ALTER TABLE fraud_rules ADD CONSTRAINT uq_fraud_rules_code_variant UNIQUE (rule_code, variant);

-- Correct score_weight values to match business logic.
-- V2 seeded approximate/placeholder weights that never matched the hard-coded
-- rule constants. Now that rules read score_weight from DB, the DB must be
-- the authoritative source. Values are derived from RiskLevel thresholds:
--   LOW <30, MEDIUM 30-59, HIGH 60-89, CRITICAL 90-100
UPDATE fraud_rules SET score_weight = 50  WHERE rule_code = 'HIGH_AMOUNT';         -- MEDIUM tier alone
UPDATE fraud_rules SET score_weight = 100 WHERE rule_code = 'BLACKLISTED_COUNTRY'; -- CRITICAL alone (capped)
UPDATE fraud_rules SET score_weight = 20  WHERE rule_code = 'SUSPICIOUS_HOUR';     -- LOW tier alone
-- VELOCITY_CHECK: score_weight is informational; actual tier scores (40/80)
-- come from medium_score / high_score parameters set below.

-- Seed parameters for every existing STABLE rule
UPDATE fraud_rules
SET parameters = '{"threshold": "10000"}'
WHERE rule_code = 'HIGH_AMOUNT';

UPDATE fraud_rules
SET parameters = '{"countries": "RU,KP,IR,SY,CU"}'
WHERE rule_code = 'BLACKLISTED_COUNTRY';

UPDATE fraud_rules
SET parameters = '{"start": "0", "end": "5"}'
WHERE rule_code = 'SUSPICIOUS_HOUR';

UPDATE fraud_rules
SET parameters = '{"window_seconds": "60", "medium_threshold": "3", "high_threshold": "5", "medium_score": "40", "high_score": "80"}'
WHERE rule_code = 'VELOCITY_CHECK';

-- ── (2) Scoring audit log ─────────────────────────────────────────────────────
-- Append-only. Never updated, never deleted (analytical / compliance use-case).

CREATE TABLE scoring_audit_log (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id   VARCHAR(64) NOT NULL,
    variant          VARCHAR(32) NOT NULL DEFAULT 'STABLE',
    raw_score        INTEGER     NOT NULL,
    fraud_score      INTEGER     NOT NULL,
    risk_level       VARCHAR(32) NOT NULL,
    triggered_rules  TEXT,           -- comma-separated rule_codes that fired
    evaluated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_transaction_id ON scoring_audit_log (transaction_id);
CREATE INDEX idx_audit_variant        ON scoring_audit_log (variant);
CREATE INDEX idx_audit_evaluated_at   ON scoring_audit_log (evaluated_at DESC);

-- ── (3) Rule set experiments ──────────────────────────────────────────────────
-- Defines a named A/B experiment with a traffic percentage routed to EXPERIMENT.
-- traffic_pct = 10 → 10% of transactions evaluated with EXPERIMENT rules.
-- Deterministic routing: abs(hash(transactionId)) % 100 < traffic_pct → EXPERIMENT.

CREATE TABLE rule_set_experiments (
    id               UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    name             VARCHAR(128) NOT NULL,
    description      TEXT,
    traffic_pct      SMALLINT     NOT NULL DEFAULT 10
                         CONSTRAINT ck_traffic_pct CHECK (traffic_pct BETWEEN 0 AND 100),
    active           BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_by       VARCHAR(128),
    activated_at     TIMESTAMPTZ,
    deactivated_at   TIMESTAMPTZ
);
