package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Flags transactions whose amount exceeds a configurable threshold.
 *
 * Score: +50
 * Config: app.rules.high-amount.threshold (default 10,000)
 */
@Component
@Order(1)
@Slf4j
public class HighAmountRule implements FraudRule {

    private static final String RULE_CODE = "HIGH_AMOUNT";
    private static final int    SCORE     = 50;

    @Value("${app.rules.high-amount.threshold:10000}")
    private BigDecimal threshold;

    @Override
    public RuleResult evaluate(TransactionEvent event) {
        boolean exceeded = event.amount().compareTo(threshold) > 0;

        if (exceeded) {
            log.debug("[{}] TRIGGERED — amount={} threshold={} transactionId={}",
                    RULE_CODE, event.amount(), threshold, event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, SCORE,
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
