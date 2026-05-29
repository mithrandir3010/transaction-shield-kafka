package com.transactionshield.engine.rule;

import com.transactionshield.common.event.TransactionEvent;

/**
 * Strategy interface for fraud detection rules.
 *
 * Each implementation is a Spring @Component collected by FraudRuleEngine
 * via {@code List<FraudRule>} injection. Adding a new rule requires only
 * creating a new @Component — the engine is unchanged.
 *
 * Implementations must NOT hold mutable threshold state. All parameters
 * are read from the {@link RuleConfig} snapshot passed at evaluation time,
 * which is refreshed periodically from the DB without service restart.
 */
public interface FraudRule {

    /**
     * Evaluates the rule using the DB-sourced configuration snapshot.
     *
     * @param event  the transaction to evaluate
     * @param config DB-backed configuration (score weight + typed parameters)
     * @return RuleResult with triggered=true and score from config if condition matched
     */
    RuleResult evaluate(TransactionEvent event, RuleConfig config);

    /**
     * Unique machine-readable code for this rule.
     * Must match the rule_code column in the fraud_rules table.
     */
    String getRuleCode();
}
