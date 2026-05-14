package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Flags transactions originating from countries on the blocklist.
 *
 * Score: +100
 * Config: app.rules.blacklisted-countries (comma-separated ISO 3166-1 alpha-2 codes)
 *
 * The blocklist is externalized so it can be updated without redeployment
 * (e.g. via environment variable or ConfigMap in Kubernetes).
 */
@Component
@Order(2)
@Slf4j
public class BlacklistedCountryRule implements FraudRule {

    private static final String RULE_CODE = "BLACKLISTED_COUNTRY";
    private static final int    SCORE     = 100;

    private final Set<String> blocklist;

    public BlacklistedCountryRule(
            @Value("${app.rules.blacklisted-countries:RU,KP,IR,SY,CU}") String raw) {
        this.blocklist = Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public RuleResult evaluate(TransactionEvent event) {
        String country = event.country().toUpperCase();
        boolean blocked = blocklist.contains(country);

        if (blocked) {
            log.debug("[{}] TRIGGERED — country={} transactionId={}",
                    RULE_CODE, country, event.transactionId());
            return RuleResult.triggered(
                    RULE_CODE, SCORE,
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
