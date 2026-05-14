package com.transactionshield.engine.rule.impl;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.rule.FraudRule;
import com.transactionshield.engine.rule.RuleResult;
import com.transactionshield.engine.service.VelocityCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Velocity Check — Redis Sliding Window tabanlı hız kontrolü.
 *
 * Bir kullanıcının belirli bir zaman penceresinde çok fazla işlem
 * yapmasını iki kademeli eşik ile tespit eder.
 *
 * Puan tablosu (pencere: 60 sn):
 *   count <= 3              → tetiklenmez
 *   count  > 3  && <= 5    → +40 puan  (şüpheli hız)
 *   count  > 5             → +80 puan  (yüksek hız, fraud olasılığı yüksek)
 *
 * Tüm eşik ve skor değerleri application.yml'den okunur →
 * yeniden deploy gerekmeden tune edilebilir.
 *
 * @Order(4): Mevcut 3 kuraldan sonra çalışır.
 * FraudRuleEngine'e hiçbir değişiklik gerekmez — sadece @Component olması yeterli.
 */
@Component
@Order(4)
@RequiredArgsConstructor
@Slf4j
public class VelocityRule implements FraudRule {

    private static final String RULE_CODE = "VELOCITY_CHECK";

    private final VelocityCheckService velocityCheckService;

    // ── Konfigürasyon (application.yml'den, runtime'da override edilebilir) ──

    @Value("${app.rules.velocity.window-seconds:60}")
    private long windowSeconds;

    @Value("${app.rules.velocity.medium-threshold:3}")
    private int mediumThreshold;   // Bu eşiği AŞARSA medium score

    @Value("${app.rules.velocity.high-threshold:5}")
    private int highThreshold;     // Bu eşiği AŞARSA high score

    @Value("${app.rules.velocity.medium-score:40}")
    private int mediumScore;

    @Value("${app.rules.velocity.high-score:80}")
    private int highScore;

    @Override
    public RuleResult evaluate(TransactionEvent event) {
        long windowMs    = windowSeconds * 1_000L;
        long ttlSeconds  = windowSeconds * 2;    // key TTL = pencere * 2 (güvenlik marjı)

        int count = velocityCheckService.countInWindow(
                event.userId(),
                event.transactionId(),
                windowMs,
                ttlSeconds
        );

        // ── Yüksek hız (count > 5) ──────────────────────────────────
        if (count > highThreshold) {
            String reason = "%d işlem %d sn pencerede (yüksek eşik: %d)"
                    .formatted(count, windowSeconds, highThreshold);
            log.debug("[{}] HIGH TRIGGERED — userId={} count={} score={}",
                    RULE_CODE, event.userId(), count, highScore);
            return RuleResult.triggered(RULE_CODE, highScore, reason);
        }

        // ── Orta hız (count > 3 && count <= 5) ─────────────────────
        if (count > mediumThreshold) {
            String reason = "%d işlem %d sn pencerede (orta eşik: %d)"
                    .formatted(count, windowSeconds, mediumThreshold);
            log.debug("[{}] MEDIUM TRIGGERED — userId={} count={} score={}",
                    RULE_CODE, event.userId(), count, mediumScore);
            return RuleResult.triggered(RULE_CODE, mediumScore, reason);
        }

        // ── Normal hız ───────────────────────────────────────────────
        log.debug("[{}] not triggered — userId={} count={}", RULE_CODE, event.userId(), count);
        return RuleResult.notTriggered(RULE_CODE);
    }

    @Override
    public String getRuleCode() {
        return RULE_CODE;
    }
}
