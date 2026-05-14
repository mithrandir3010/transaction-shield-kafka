package com.transactionshield.engine.scoring;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Orchestrates all registered FraudRule beans via Strategy Pattern.
 *
 * Spring auto-collects every @Component implementing FraudRule into
 * this list, ordered by @Order. To add a new rule: create a new
 * @Component class — this engine requires zero changes.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FraudRuleEngine {

    private final List<FraudRule> rules;

    public ScoringResult evaluate(TransactionEvent event) {
        log.debug("Evaluating {} rules for transactionId={}", rules.size(), event.transactionId());

        List<RuleResult> allResults = rules.stream()
                .map(rule -> rule.evaluate(event))
                .toList();

        List<RuleResult> triggered = allResults.stream()
                .filter(RuleResult::triggered)
                .toList();

        int rawScore = triggered.stream()
                .mapToInt(RuleResult::score)
                .sum();

        int fraudScore = Math.min(rawScore, 100);
        RiskLevel riskLevel = RiskLevel.from(fraudScore);

        log.info("Scoring complete — transactionId={} rawScore={} fraudScore={} riskLevel={} triggeredRules={}",
                event.transactionId(), rawScore, fraudScore, riskLevel,
                triggered.stream().map(RuleResult::ruleCode).toList());

        return new ScoringResult(event.transactionId(), rawScore, fraudScore, riskLevel, triggered);
    }
}
