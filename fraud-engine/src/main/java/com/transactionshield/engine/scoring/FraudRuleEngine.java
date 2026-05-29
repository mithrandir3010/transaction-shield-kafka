package com.transactionshield.engine.scoring;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.rule.RuleResult;
import com.transactionshield.engine.service.AuditLogService;
import com.transactionshield.engine.service.RuleConfigService;
import com.transactionshield.engine.service.RuleSetExperimentService;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Orchestrates all registered {@link FraudRule} beans via Strategy Pattern.
 *
 * Evaluation flow per transaction:
 *   1. Select variant (STABLE or EXPERIMENT) via hash-based A/B routing
 *   2. Load DB-backed config for each rule + selected variant
 *   3. Skip rules that are disabled or have no config for that variant
 *   4. Accumulate scores from triggered rules → cap at 100 → derive RiskLevel
 *   5. Write async audit log entry with the variant used
 *   6. Return ScoringResult (includes variant for downstream use)
 *
 * Adding a new rule: create a @Component implementing FraudRule — zero changes here.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FraudRuleEngine {

    private final List<FraudRule>         rules;
    private final RuleConfigService       ruleConfigService;
    private final RuleSetExperimentService experimentService;
    private final AuditLogService         auditLogService;
    private final MeterRegistry           meterRegistry;

    public ScoringResult evaluate(TransactionEvent event) {
        // ── Select variant ─────────────────────────────────────────────
        String variant = experimentService.selectVariant(event.transactionId());

        log.debug("Evaluating {} rules — transactionId={} variant={}",
                rules.size(), event.transactionId(), variant);

        // ── Evaluate each rule with its DB-backed config ────────────────
        List<RuleResult> allResults = rules.stream()
                .map(rule -> evaluateRule(rule, event, variant))
                .toList();

        // ── Aggregate ──────────────────────────────────────────────────
        List<RuleResult> triggered = allResults.stream()
                .filter(RuleResult::triggered)
                .toList();

        int rawScore   = triggered.stream().mapToInt(RuleResult::score).sum();
        int fraudScore = Math.min(rawScore, 100);
        RiskLevel riskLevel = RiskLevel.from(fraudScore);

        // ── Metrics ────────────────────────────────────────────────────
        triggered.forEach(r -> meterRegistry.counter("fraud.rule.hit.total",
                "rule", r.ruleCode(), "variant", variant).increment());

        DistributionSummary.builder("fraud.score.distribution")
                .baseUnit("points")
                .tag("variant", variant)
                .register(meterRegistry)
                .record(fraudScore);

        meterRegistry.counter("transaction.processed.total",
                "risk_level", riskLevel.name(), "variant", variant).increment();

        log.info("Scoring — transactionId={} variant={} rawScore={} fraudScore={} riskLevel={} triggered={}",
                event.transactionId(), variant, rawScore, fraudScore, riskLevel,
                triggered.stream().map(RuleResult::ruleCode).toList());

        ScoringResult result = new ScoringResult(
                event.transactionId(), rawScore, fraudScore, riskLevel, triggered, variant);

        // ── Audit log (async, non-blocking) ────────────────────────────
        auditLogService.record(result);

        return result;
    }

    private RuleResult evaluateRule(FraudRule rule, TransactionEvent event, String variant) {
        Optional<RuleConfig> configOpt = ruleConfigService.getConfig(rule.getRuleCode(), variant);

        if (configOpt.isEmpty()) {
            log.warn("[RULE-ENGINE] No config found for rule={} variant={} — skipping",
                    rule.getRuleCode(), variant);
            return RuleResult.notTriggered(rule.getRuleCode());
        }

        RuleConfig config = configOpt.get();
        if (!config.enabled()) {
            log.debug("[RULE-ENGINE] Rule {} disabled (variant={}) — skipping",
                    rule.getRuleCode(), variant);
            return RuleResult.notTriggered(rule.getRuleCode());
        }

        return rule.evaluate(event, config);
    }
}
