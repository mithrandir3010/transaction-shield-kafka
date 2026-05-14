package com.transactionshield.engine.rule;

/**
 * Immutable result from a single FraudRule evaluation.
 *
 * @param ruleCode  unique rule identifier (e.g. "HIGH_AMOUNT")
 * @param triggered whether the rule's condition matched
 * @param score     points added to the fraud score (0 if not triggered)
 * @param reason    human-readable explanation for logs and alerts
 */
public record RuleResult(
        String ruleCode,
        boolean triggered,
        int score,
        String reason
) {
    /** Convenience factory for a rule that did NOT fire. */
    public static RuleResult notTriggered(String ruleCode) {
        return new RuleResult(ruleCode, false, 0, "Rule condition not met");
    }

    /** Convenience factory for a rule that fired. */
    public static RuleResult triggered(String ruleCode, int score, String reason) {
        return new RuleResult(ruleCode, true, score, reason);
    }
}
