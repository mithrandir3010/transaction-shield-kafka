package com.transactionshield.engine.scoring;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.engine.rule.RuleResult;

import java.util.List;

/**
 * Aggregated output of the FraudRuleEngine for a single transaction.
 *
 * @param transactionId  links result back to the original event
 * @param rawScore       uncapped sum of all triggered rule scores
 * @param fraudScore     Math.min(rawScore, 100) — stored in DB and Avro event
 * @param riskLevel      derived from fraudScore via RiskLevel.from()
 * @param triggeredRules only the rules that fired (triggered=true)
 * @param variant        which rule set was used: "STABLE" or "EXPERIMENT"
 */
public record ScoringResult(
        String transactionId,
        int rawScore,
        int fraudScore,
        RiskLevel riskLevel,
        List<RuleResult> triggeredRules,
        String variant
) {
    public boolean isSuspicious() {
        return riskLevel == RiskLevel.HIGH || riskLevel == RiskLevel.CRITICAL;
    }

    public List<String> triggeredRuleCodes() {
        return triggeredRules.stream()
                .map(RuleResult::ruleCode)
                .toList();
    }
}
