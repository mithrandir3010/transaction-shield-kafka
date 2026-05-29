-- Widen SMALLINT columns to INTEGER to match Java entity field types.
--
-- FraudRuleEntity.scoreWeight (int) requires INTEGER; V1 created SMALLINT.
-- RuleSetExperiment.trafficPct (int) requires INTEGER; V3 created SMALLINT.
--
-- ALTER COLUMN ... TYPE INTEGER is safe in PostgreSQL — it widens the range
-- from [-32768, 32767] to [-2^31, 2^31-1] with no data loss.
-- Identical fix was applied to alert-service fraud_score in V2.

ALTER TABLE fraud_rules          ALTER COLUMN score_weight TYPE INTEGER;
ALTER TABLE rule_set_experiments ALTER COLUMN traffic_pct  TYPE INTEGER;
