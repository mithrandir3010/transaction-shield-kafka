INSERT INTO fraud_rules (rule_code, description, score_weight, enabled) VALUES
    ('HIGH_AMOUNT',         'Transaction amount exceeds threshold',              30, TRUE),
    ('VELOCITY_CHECK',      'Too many transactions in a short time window',      25, TRUE),
    ('BLACKLISTED_COUNTRY', 'Transaction originates from a blacklisted country', 40, TRUE),
    ('SUSPICIOUS_HOUR',     'Transaction at unusual hours (02:00–05:00)',         15, TRUE),
    ('NEW_DEVICE',          'First-time device fingerprint for this account',    10, TRUE);
