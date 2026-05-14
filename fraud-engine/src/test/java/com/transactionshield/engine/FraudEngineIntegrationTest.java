package com.transactionshield.engine;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-End Integration Testi
 *
 * Senaryo:
 *   TransactionEvent → transactions.raw (Kafka)
 *       → fraud-engine consumer → FraudRuleEngine
 *       → ScoredTransactionEvent → transactions.scored (Kafka)
 *       → ScoredEventCollector (test consumer) → assertion
 *
 * Her test: gerçek Kafka (Testcontainers), gerçek Redis, gerçek PostgreSQL.
 * Hiçbir mock yok — sistemin davranışını tam olarak doğrular.
 */
@DisplayName("Fraud Engine — End-to-End Integration Tests")
class FraudEngineIntegrationTest extends AbstractIntegrationTest {

    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(15);

    // ── Test 1: HIGH_AMOUNT kuralı ────────────────────────────────────

    @Test
    @DisplayName("15.000 USD işlem → HighAmountRule tetiklenir, fraudScore=50, MEDIUM risk")
    void whenHighAmountTransaction_thenHighAmountRuleTriggered() throws Exception {
        // Given
        TransactionEvent event = buildEvent("u-ha-001", BigDecimal.valueOf(15_000), "US");

        // When
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then
        ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

        assertThat(scored).as("fraud-engine should publish a scored event").isNotNull();
        assertThat(scored.transactionId()).isEqualTo(event.transactionId());
        assertThat(scored.triggeredRules()).contains("HIGH_AMOUNT");
        assertThat(scored.fraudScore()).isEqualTo(50);
        assertThat(scored.riskLevel()).isEqualTo(RiskLevel.MEDIUM);
        assertThat(scored.scoredAt()).isNotNull();
    }

    // ── Test 2: BLACKLISTED_COUNTRY kuralı ───────────────────────────

    @Test
    @DisplayName("RU kaynaklı işlem → BlacklistedCountryRule tetiklenir, fraudScore=100, CRITICAL")
    void whenBlacklistedCountryTransaction_thenCriticalRisk() throws Exception {
        // Given
        TransactionEvent event = buildEvent("u-bc-001", BigDecimal.valueOf(200), "RU");

        // When
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then
        ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

        assertThat(scored).isNotNull();
        assertThat(scored.triggeredRules()).contains("BLACKLISTED_COUNTRY");
        assertThat(scored.fraudScore()).isEqualTo(100);   // maksimum, capped
        assertThat(scored.riskLevel()).isEqualTo(RiskLevel.CRITICAL);
    }

    // ── Test 3: SUSPICIOUS_HOUR kuralı ───────────────────────────────

    @Test
    @DisplayName("Gece 02:30 UTC işlemi → NightTransactionRule tetiklenir, score=20, LOW risk")
    void whenNightTimeTransaction_thenNightRuleTriggered() throws Exception {
        // Given: timestamp 02:30 UTC — NightTransactionRule penceresi (00:00–05:00)
        TransactionEvent event = buildNightEvent("u-nt-001", BigDecimal.valueOf(100), "US");

        // When
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then
        ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

        assertThat(scored).isNotNull();
        assertThat(scored.triggeredRules()).contains("SUSPICIOUS_HOUR");
        assertThat(scored.fraudScore()).isEqualTo(20);
        assertThat(scored.riskLevel()).isEqualTo(RiskLevel.LOW);
    }

    // ── Test 4: Çoklu kural — skor birikimliliği ─────────────────────

    @Test
    @DisplayName("15.000 USD + RU → HIGH_AMOUNT+BLACKLISTED_COUNTRY, rawScore=150 → fraudScore=100 (cap)")
    void whenMultipleRulesTriggered_thenScoresCumulativeAndCapped() throws Exception {
        // Given: hem HIGH_AMOUNT (50) hem BLACKLISTED_COUNTRY (100) → raw=150 → cap=100
        TransactionEvent event = buildEvent("u-multi-001", BigDecimal.valueOf(15_000), "RU");

        // When
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then
        ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

        assertThat(scored).isNotNull();
        assertThat(scored.triggeredRules())
                .as("Both HIGH_AMOUNT and BLACKLISTED_COUNTRY should fire")
                .containsExactlyInAnyOrder("HIGH_AMOUNT", "BLACKLISTED_COUNTRY");
        assertThat(scored.rawScore())
                .as("rawScore = 50 (HIGH_AMOUNT) + 100 (BLACKLISTED_COUNTRY)")
                .isEqualTo(150);
        assertThat(scored.fraudScore())
                .as("fraudScore is capped at 100")
                .isEqualTo(100);
        assertThat(scored.riskLevel()).isEqualTo(RiskLevel.CRITICAL);
    }

    // ── Test 5: Normal işlem — hiçbir kural tetiklenmemeli ───────────

    @Test
    @DisplayName("500 USD, US, gündüz → hiçbir kural tetiklenmez, riskLevel=LOW")
    void whenNormalTransaction_thenNoRulesTriggered() throws Exception {
        // Given: küçük miktar, temiz ülke, gündüz saati
        TransactionEvent event = buildEvent("u-clean-" + UUID.randomUUID(), BigDecimal.valueOf(500), "US");

        // When
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then
        ScoredTransactionEvent scored = collector.poll(EVENT_TIMEOUT);

        assertThat(scored).isNotNull();
        assertThat(scored.triggeredRules())
                .as("No fraud rules should trigger for a clean transaction")
                .doesNotContain("HIGH_AMOUNT", "BLACKLISTED_COUNTRY", "SUSPICIOUS_HOUR");
        assertThat(scored.fraudScore()).isEqualTo(0);
        assertThat(scored.riskLevel()).isEqualTo(RiskLevel.LOW);
    }
}
