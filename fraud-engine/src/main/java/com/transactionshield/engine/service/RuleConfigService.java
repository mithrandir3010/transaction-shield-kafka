package com.transactionshield.engine.service;

import com.transactionshield.engine.entity.FraudRuleEntity;
import com.transactionshield.engine.repository.FraudRuleRepository;
import com.transactionshield.engine.rule.RuleConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory cache of rule configurations loaded from the fraud_rules table.
 *
 * Cache key: {@code "<ruleCode>:<variant>"} (e.g. {@code "HIGH_AMOUNT:STABLE"}).
 * Refreshed every {@code app.rules.refresh-interval-ms} milliseconds (default 5 min)
 * via {@link Scheduled}. The initialDelay=0 guarantees the first load runs at
 * application start before any Kafka messages are processed.
 *
 * Config lookups fall back from EXPERIMENT → STABLE so rules without an
 * experiment override still use their production configuration.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RuleConfigService {

    private final FraudRuleRepository ruleRepository;

    private volatile Map<String, RuleConfig> cache = new ConcurrentHashMap<>();

    @Scheduled(fixedDelayString = "${app.rules.refresh-interval-ms:300000}", initialDelay = 0)
    @Transactional(readOnly = true)
    public void refresh() {
        List<FraudRuleEntity> entities = ruleRepository.findAll();

        Map<String, RuleConfig> updated = entities.stream()
                .collect(Collectors.toMap(
                        e -> cacheKey(e.getRuleCode(), e.getVariant()),
                        e -> new RuleConfig(
                                e.getRuleCode(),
                                e.getScoreWeight(),
                                e.isEnabled(),
                                e.getVariant(),
                                e.getParameters() != null ? e.getParameters() : Map.of()
                        ),
                        (a, b) -> b  // last write wins if duplicates somehow exist
                ));

        this.cache = updated;
        log.info("[RULE-CONFIG] Refreshed {} rule configs from DB", updated.size());
    }

    /**
     * Returns the rule config for the given rule code and variant.
     * Falls back to STABLE if no EXPERIMENT config exists for the rule.
     */
    public Optional<RuleConfig> getConfig(String ruleCode, String variant) {
        RuleConfig config = cache.get(cacheKey(ruleCode, variant));
        if (config == null && !"STABLE".equals(variant)) {
            config = cache.get(cacheKey(ruleCode, "STABLE"));
        }
        return Optional.ofNullable(config);
    }

    /** Returns all cached configs for API listing. */
    public List<RuleConfig> getAllConfigs() {
        return List.copyOf(cache.values());
    }

    private static String cacheKey(String ruleCode, String variant) {
        return ruleCode + ":" + variant;
    }
}
