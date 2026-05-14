package com.transactionshield.engine.rule;

import com.transactionshield.common.event.TransactionEvent;

/**
 * Strategy interface for fraud detection rules.
 *
 * Each implementation is a Spring @Component. The FraudRuleEngine
 * auto-collects ALL beans via List<FraudRule> injection — adding
 * a new rule requires only creating a new @Component class here.
 *
 * Use @Order on implementations to control evaluation sequence
 * (affects log ordering; score is always cumulative regardless).
 */
public interface FraudRule {

    /**
     * Evaluates the rule against the given transaction event.
     *
     * @return RuleResult with triggered=true and the rule's score weight
     *         if the condition matched, or triggered=false with score=0.
     */
    RuleResult evaluate(TransactionEvent event);

    /**
     * Unique machine-readable code for this rule.
     * Must match the rule_code in the fraud_rules table.
     */
    String getRuleCode();
}
