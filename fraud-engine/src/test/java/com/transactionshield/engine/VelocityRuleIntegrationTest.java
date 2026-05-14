package com.transactionshield.engine;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * VelocityRule — Redis Sliding Window Integration Testi
 *
 * Strateji:
 *   Sıralı publish-bekle döngüsü: her event publish edildikten SONRA
 *   scored event beklenir. Bu sayede:
 *   1. Redis ZSET'i sıralı güncellenir (window count doğru artar)
 *   2. Test deterministik olur (race condition riski ortadan kalkar)
 *
 * Beklenen puan skalası (window=60sn, mediumThreshold=3, highThreshold=5):
 *   İşlem 1–3 : count 1,2,3 → VELOCITY_CHECK tetiklenmez
 *   İşlem 4–5 : count 4,5   → VELOCITY_CHECK +40 puan (medium tier)
 *   İşlem 6   : count 6     → VELOCITY_CHECK +80 puan (high tier)
 */
@DisplayName("VelocityRule — Sliding Window Integration Tests")
class VelocityRuleIntegrationTest extends AbstractIntegrationTest {

    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(15);

    @Test
    @DisplayName("6 ardışık işlem → velocity count kademeli yükselir ve 6. işlem HIGH velocity'e ulaşır")
    void whenSixRapidTransactions_thenVelocityEscalatesProgressively() throws Exception {

        // Benzersiz userId: testler arası Redis key çakışmasını önler
        String userId = "velocity-user-" + UUID.randomUUID().toString().substring(0, 8);

        // Beklenen durum tablosu (0-indexed, count=i+1)
        record Expectation(int txIndex, boolean velocityExpected, int minScore, String description) {}

        List<Expectation> expectations = List.of(
                new Expectation(0, false, 0,  "count=1 → tetiklenmez"),
                new Expectation(1, false, 0,  "count=2 → tetiklenmez"),
                new Expectation(2, false, 0,  "count=3 → tetiklenmez (eşikte değil, aşmadı)"),
                new Expectation(3, true,  40, "count=4 → mediumThreshold(3) aşıldı, +40"),
                new Expectation(4, true,  40, "count=5 → hâlâ medium tier, +40"),
                new Expectation(5, true,  80, "count=6 → highThreshold(5) aşıldı, +80")
        );

        List<ScoredTransactionEvent> results = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            TransactionEvent event = buildEvent(userId, BigDecimal.valueOf(100), "US");

            // Publish ve Kafka onayını bekle (at-least-once)
            rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

            // Scored event'i bekle — Redis ZSET bu event için güncellendikten SONRA gelir
            ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

            assertThat(scored)
                    .as("İşlem #%d scored event üretmeli (userId=%s)", i + 1, userId)
                    .isNotNull();

            results.add(scored);
        }

        // ── Assertion: her işlem için beklenen durumu doğrula ─────────
        for (Expectation exp : expectations) {
            ScoredTransactionEvent scored = results.get(exp.txIndex());

            if (exp.velocityExpected()) {
                assertThat(scored.triggeredRules())
                        .as("[İşlem #%d] %s — VELOCITY_CHECK tetiklenmeli", exp.txIndex() + 1, exp.description())
                        .contains("VELOCITY_CHECK");
                assertThat(scored.fraudScore())
                        .as("[İşlem #%d] fraudScore >= %d olmalı", exp.txIndex() + 1, exp.minScore())
                        .isGreaterThanOrEqualTo(exp.minScore());
            } else {
                assertThat(scored.triggeredRules())
                        .as("[İşlem #%d] %s — VELOCITY_CHECK tetiklenmemeli", exp.txIndex() + 1, exp.description())
                        .doesNotContain("VELOCITY_CHECK");
            }
        }

        // ── Özet doğrulama: 6. işlem HIGH/CRITICAL olmalı ─────────────
        ScoredTransactionEvent lastEvent = results.get(5);
        assertThat(lastEvent.riskLevel())
                .as("6. işlem HIGH veya CRITICAL risk seviyesinde olmalı")
                .isIn(RiskLevel.HIGH, RiskLevel.CRITICAL);
        assertThat(lastEvent.triggeredRules())
                .as("6. işlemde VELOCITY_CHECK tetiklenmeli")
                .contains("VELOCITY_CHECK");
    }

    @Test
    @DisplayName("Farklı kullanıcılar birbirinin velocity penceresini etkilemez")
    void whenTwoUsersSendTransactions_thenVelocityWindowsAreIsolated() throws Exception {
        String userA = "velocity-userA-" + UUID.randomUUID().toString().substring(0, 8);
        String userB = "velocity-userB-" + UUID.randomUUID().toString().substring(0, 8);

        // userA: 4 işlem → velocity tetiklenir
        for (int i = 0; i < 4; i++) {
            rawEventTemplate.send("transactions.raw",
                    buildEvent(userA, BigDecimal.valueOf(100), "US").transactionId(),
                    buildEvent(userA, BigDecimal.valueOf(100), "US")).get();
        }

        // userB: 1 işlem — userA'nın penceresi bunu etkilememeli
        TransactionEvent userBEvent = buildEvent(userB, BigDecimal.valueOf(100), "US");
        rawEventTemplate.send("transactions.raw", userBEvent.transactionId(), userBEvent).get();

        // 5 scored event topla
        List<ScoredTransactionEvent> allScored = collector.pollN(5, EVENT_TIMEOUT);

        assertThat(allScored).hasSize(5);

        // userB'nin event'i: VELOCITY_CHECK tetiklenmemiş olmalı
        ScoredTransactionEvent userBScored = allScored.stream()
                .filter(e -> e.userId().equals(userB))
                .findFirst()
                .orElse(null);

        assertThat(userBScored).as("userB scored event bulunmalı").isNotNull();
        assertThat(userBScored.triggeredRules())
                .as("userB sadece 1 işlem yaptı, velocity tetiklenmemeli")
                .doesNotContain("VELOCITY_CHECK");
    }
}
