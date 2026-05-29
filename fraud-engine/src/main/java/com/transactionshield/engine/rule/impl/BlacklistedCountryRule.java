package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flags transactions originating from countries on the block-list.
 *
 * DB parameters (fraud_rules.parameters JSONB):
 *   countries — comma-separated ISO 3166-1 alpha-2 codes (default: RU,KP,IR,SY,CU)
 *
 * The block-list and score weight are hot-reloaded from the DB, so adding or
 * removing countries takes effect within the next config refresh cycle.
 */
@Component
@Order(2)
@Slf4j
public class BlacklistedCountryRule implements FraudRule {

    private static final String RULE_CODE = "BLACKLISTED_COUNTRY";

    @Override
    public RuleResult evaluate(TransactionEvent event, RuleConfig config) {
        String raw = config.param("countries", "RU,KP,IR,SY,CU");
        Set<String> blocklist = Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .collect(Collectors.toUnmodifiableSet());

        String country = event.country().toUpperCase();
        boolean blocked = blocklist.contains(country);

        if (blocked) {
            log.debug("[{}] TRIGGERED — country={} variant={} transactionId={}",
                    RULE_CODE, country, config.variant(), event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, config.scoreWeight(),
                    "Country %s is on the blocklist".formatted(country)
            );
        }
        return RuleResult.notTriggered(RULE_CODE);
    }

    @Override
    public String getRuleCode() {
        return RULE_CODE;
    }
}
