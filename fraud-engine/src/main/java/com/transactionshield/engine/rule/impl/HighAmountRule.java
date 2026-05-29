package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Flags transactions whose amount exceeds the configured threshold.
 *
 * DB parameters (fraud_rules.parameters JSONB):
 *   threshold — amount above which the rule fires (default: 10 000)
 *
 * score_weight and threshold can be changed in the DB; the engine picks up
 * the new values on the next config refresh (default every 5 minutes).
 */
@Component
@Order(1)
@Slf4j
public class HighAmountRule implements FraudRule {

    private static final String RULE_CODE = "HIGH_AMOUNT";

    @Override
    public RuleResult evaluate(TransactionEvent event, RuleConfig config) {
        BigDecimal threshold = config.param("threshold", BigDecimal.valueOf(10_000));
        boolean exceeded = event.amount().compareTo(threshold) > 0;

        if (exceeded) {
            log.debug("[{}] TRIGGERED — amount={} threshold={} variant={} transactionId={}",
                    RULE_CODE, event.amount(), threshold, config.variant(), event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, config.scoreWeight(),
                    "Amount %s exceeds threshold %s".formatted(event.amount(), threshold)
            );
        }
        return RuleResult.notTriggered(RULE_CODE);
    }

    @Override
    public String getRuleCode() {
        return RULE_CODE;
    }
}
