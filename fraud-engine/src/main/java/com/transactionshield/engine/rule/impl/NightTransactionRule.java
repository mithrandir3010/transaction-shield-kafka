package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;

/**
 * Flags transactions occurring during off-hours (default: 00:00–05:00 UTC).
 *
 * Score: +20
 * Config: app.rules.night-hours.start / end (inclusive start, exclusive end)
 *
 * Why UTC? Financial systems must be timezone-agnostic for global transactions.
 * The alert-service can localize the display time for analysts.
 */
@Component
@Order(3)
@Slf4j
public class NightTransactionRule implements FraudRule {

    private static final String RULE_CODE = "SUSPICIOUS_HOUR";
    private static final int    SCORE     = 20;

    @Value("${app.rules.night-hours.start:0}")
    private int nightStart;  // inclusive, e.g. 0 = midnight

    @Value("${app.rules.night-hours.end:5}")
    private int nightEnd;    // exclusive, e.g. 5 = 05:00 (so 00:00–04:59)

    @Override
    public RuleResult evaluate(TransactionEvent event) {
        int hour = event.timestamp()
                .atZone(ZoneOffset.UTC)
                .getHour();

        boolean isNight = hour >= nightStart && hour < nightEnd;

        if (isNight) {
            log.debug("[{}] TRIGGERED — hour={}:xx UTC window=[{}-{}) transactionId={}",
                    RULE_CODE, hour, nightStart, nightEnd, event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, SCORE,
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
