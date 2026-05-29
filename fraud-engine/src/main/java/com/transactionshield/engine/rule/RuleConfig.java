package com.transactionshield.engine.rule;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Immutable snapshot of a rule's DB configuration passed into each evaluation.
 *
 * Rules read their score weight and thresholds from this object instead of
 * hard-coded constants or @Value-injected fields. Config snapshots are refreshed
 * periodically by {@link com.transactionshield.engine.service.RuleConfigService}
 * so threshold changes take effect without a service restart.
 *
 * Parameter access methods have typed fallbacks for missing or malformed values.
 */
public record RuleConfig(
        String ruleCode,
        int scoreWeight,
        boolean enabled,
        String variant,
        Map<String, String> parameters
) {

    public BigDecimal param(String key, BigDecimal fallback) {
        String v = parameters != null ? parameters.get(key) : null;
        if (v == null) return fallback;
        try {
            return new BigDecimal(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public int param(String key, int fallback) {
        String v = parameters != null ? parameters.get(key) : null;
        if (v == null) return fallback;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public long param(String key, long fallback) {
        String v = parameters != null ? parameters.get(key) : null;
        if (v == null) return fallback;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public String param(String key, String fallback) {
        if (parameters == null) return fallback;
        return parameters.getOrDefault(key, fallback);
    }
}
