package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.rule.RuleResult;
import com.transactionshield.engine.service.VelocityCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Two-tier velocity check using Redis sliding window.
 *
 * DB parameters (fraud_rules.parameters JSONB):
 *   window_seconds   — sliding window width (default: 60)
 *   medium_threshold — count > this fires medium tier (default: 3)
 *   high_threshold   — count > this fires high tier   (default: 5)
 *   medium_score     — score for medium tier           (default: 40)
 *   high_score       — score for high tier             (default: 80)
 *
 * score_weight in DB is informational for this rule; actual scores come from
 * medium_score and high_score parameters to support the two-tier design.
 */
@Component
@Order(4)
@RequiredArgsConstructor
@Slf4j
public class VelocityRule implements FraudRule {

    private static final String RULE_CODE = "VELOCITY_CHECK";

    private final VelocityCheckService velocityCheckService;

    @Override
    public RuleResult evaluate(TransactionEvent event, RuleConfig config) {
        long windowSeconds   = config.param("window_seconds",   60L);
        int  mediumThreshold = config.param("medium_threshold", 3);
        int  highThreshold   = config.param("high_threshold",   5);
        int  mediumScore     = config.param("medium_score",     40);
        int  highScore       = config.param("high_score",       80);

        long windowMs   = windowSeconds * 1_000L;
        long ttlSeconds = windowSeconds * 2;

        int count = velocityCheckService.countInWindow(
                event.userId(), event.transactionId(), windowMs, ttlSeconds);

        if (count > highThreshold) {
            log.debug("[{}] HIGH — userId={} count={} score={} variant={}",
                    RULE_CODE, event.userId(), count, highScore, config.variant());
            return RuleResult.triggered(RULE_CODE, highScore,
                    "%d tx in %ds window (high threshold: >%d)"
                            .formatted(count, windowSeconds, highThreshold));
        }

        if (count > mediumThreshold) {
            log.debug("[{}] MEDIUM — userId={} count={} score={} variant={}",
                    RULE_CODE, event.userId(), count, mediumScore, config.variant());
            return RuleResult.triggered(RULE_CODE, mediumScore,
                    "%d tx in %ds window (medium threshold: >%d)"
                            .formatted(count, windowSeconds, mediumThreshold));
        }

        log.debug("[{}] not triggered — userId={} count={}", RULE_CODE, event.userId(), count);
        return RuleResult.notTriggered(RULE_CODE);
    }

    @Override
    public String getRuleCode() {
        return RULE_CODE;
    }
}
