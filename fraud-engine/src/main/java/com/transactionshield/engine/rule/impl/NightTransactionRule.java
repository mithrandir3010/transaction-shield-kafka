package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;

/**
 * Flags transactions occurring during off-hours (UTC).
 *
 * DB parameters (fraud_rules.parameters JSONB):
 *   start — inclusive start hour 0–23 (default: 0 = midnight)
 *   end   — exclusive end hour 0–23   (default: 5 = 05:00 UTC)
 *
 * Evaluated in UTC so global transactions are treated consistently.
 */
@Component
@Order(3)
@Slf4j
public class NightTransactionRule implements FraudRule {

    private static final String RULE_CODE = "SUSPICIOUS_HOUR";

    @Override
    public RuleResult evaluate(TransactionEvent event, RuleConfig config) {
        int nightStart = config.param("start", 0);
        int nightEnd   = config.param("end",   5);

        int hour = event.timestamp()
                .atZone(ZoneOffset.UTC)
                .getHour();

        boolean isNight = hour >= nightStart && hour < nightEnd;

        if (isNight) {
            log.debug("[{}] TRIGGERED — hour={}:xx UTC window=[{}-{}) variant={} transactionId={}",
                    RULE_CODE, hour, nightStart, nightEnd, config.variant(), event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, config.scoreWeight(),
                    "Transaction at %02d:xx UTC is within the suspicious window [%02d:00–%02d:00)"
                            .formatted(hour, nightStart, nightEnd)
            );
        }
        return RuleResult.notTriggered(RULE_CODE);
    }

    @Override
    public String getRuleCode() {
        return RULE_CODE;
    }
}
