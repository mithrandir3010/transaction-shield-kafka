-- fraud_score was SMALLINT in V1 but the JPA entity maps Integer → INTEGER.
-- Hibernate schema-validation fails when types don't match.
ALTER TABLE alerts ALTER COLUMN fraud_score TYPE INTEGER;
