package com.transactionshield.alert;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.repository.AlertRepository;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.AlertCreatedEvent;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * alert-service End-to-End Integration Testi
 *
 * Senaryo:
 *   ScoredTransactionEvent → transactions.scored (Kafka)
 *       → alert-service consumer → AlertService
 *       → Alert persist edilir (PostgreSQL)
 *       → HIGH/CRITICAL ise AlertCreatedEvent → alerts.created (Kafka)
 *
 * Her test: gerçek Kafka (Testcontainers) + gerçek PostgreSQL.
 */
@DisplayName("Alert Service — End-to-End Integration Tests")
class AlertServiceIntegrationTest extends AbstractAlertIntegrationTest {

    @Autowired AlertRepository alertRepository;

    private static final Duration WAIT          = Duration.ofSeconds(15);
    private static final Duration NEGATIVE_WAIT = Duration.ofSeconds(4);

    @Test
    @DisplayName("HIGH risk event → alert DB'ye persist edilir + AlertCreatedEvent Kafka'ya publish edilir")
    void whenHighRiskEvent_thenAlertPersistedAndEventPublished() throws Exception {
        String txId = "e2e-high-" + UUID.randomUUID();
        ScoredTransactionEvent event = buildScoredEvent(
                txId, RiskLevel.HIGH, 80, List.of("HIGH_AMOUNT"));

        publishScored(event);

        // AlertCreatedEvent bekliyoruz
        AlertCreatedEvent alertEvent = alertCreatedCollector.poll(WAIT);
        assertThat(alertEvent).as("HIGH risk için AlertCreatedEvent publish edilmeli").isNotNull();
        assertThat(alertEvent.transactionId()).isEqualTo(txId);
        assertThat(alertEvent.riskLevel()).isEqualTo("HIGH");
        assertThat(alertEvent.fraudScore()).isEqualTo(80);
        assertThat(alertEvent.triggeredRules()).contains("HIGH_AMOUNT");

        // DB'de alert oluşturuldu mu?
        Optional<Alert> savedAlert = alertRepository.findByTransactionId(txId);
        assertThat(savedAlert).isPresent();
        assertThat(savedAlert.get().getRiskLevel()).isEqualTo("HIGH");
        assertThat(savedAlert.get().getStatus()).isEqualTo("OPEN");
    }

    @Test
    @DisplayName("CRITICAL risk event → alert persist edilir + AlertCreatedEvent publish edilir")
    void whenCriticalRiskEvent_thenAlertPersistedAndEventPublished() throws Exception {
        String txId = "e2e-crit-" + UUID.randomUUID();
        ScoredTransactionEvent event = buildScoredEvent(
                txId, RiskLevel.CRITICAL, 100, List.of("BLACKLISTED_COUNTRY"));

        publishScored(event);

        AlertCreatedEvent alertEvent = alertCreatedCollector.poll(WAIT);
        assertThat(alertEvent).isNotNull();
        assertThat(alertEvent.riskLevel()).isEqualTo("CRITICAL");

        Optional<Alert> savedAlert = alertRepository.findByTransactionId(txId);
        assertThat(savedAlert).isPresent();
        assertThat(savedAlert.get().getFraudScore()).isEqualTo(100);
    }

    @Test
    @DisplayName("LOW risk event → alert DB'ye persist edilir, AlertCreatedEvent Kafka'ya yazılmaz")
    void whenLowRiskEvent_thenAlertPersistedButNoKafkaEvent() throws Exception {
        String txId = "e2e-low-" + UUID.randomUUID();
        ScoredTransactionEvent event = buildScoredEvent(txId, RiskLevel.LOW, 0, List.of());

        publishScored(event);

        // Kısa bekleme — AlertCreatedEvent gelmemeli
        AlertCreatedEvent alertEvent = alertCreatedCollector.poll(NEGATIVE_WAIT);
        assertThat(alertEvent).as("LOW risk için AlertCreatedEvent olmamalı").isNull();

        // Ama DB'de alert hâlâ persist edilmiş olmalı
        Optional<Alert> savedAlert = alertRepository.findByTransactionId(txId);
        assertThat(savedAlert).isPresent();
        assertThat(savedAlert.get().getRiskLevel()).isEqualTo("LOW");
    }

    @Test
    @DisplayName("MEDIUM risk event → alert persist edilir, AlertCreatedEvent publish edilmez")
    void whenMediumRiskEvent_thenAlertPersistedButNoKafkaEvent() throws Exception {
        String txId = "e2e-med-" + UUID.randomUUID();
        ScoredTransactionEvent event = buildScoredEvent(
                txId, RiskLevel.MEDIUM, 50, List.of("HIGH_AMOUNT"));

        publishScored(event);

        AlertCreatedEvent alertEvent = alertCreatedCollector.poll(NEGATIVE_WAIT);
        assertThat(alertEvent).as("MEDIUM risk için AlertCreatedEvent olmamalı").isNull();

        Optional<Alert> saved = alertRepository.findByTransactionId(txId);
        assertThat(saved).isPresent();
    }

    @Test
    @DisplayName("Duplicate ScoredTransactionEvent → DB'de TEK alert kalır (UNIQUE constraint)")
    void whenDuplicateScoredEvent_thenOnlyOneAlertInDb() throws Exception {
        String txId = "e2e-dup-" + UUID.randomUUID();
        ScoredTransactionEvent event = buildScoredEvent(
                txId, RiskLevel.HIGH, 80, List.of("HIGH_AMOUNT"));

        // Aynı event iki kez publish (Kafka at-least-once simülasyonu)
        publishScored(event);
        publishScored(event);

        // İlk AlertCreatedEvent bekliyoruz
        AlertCreatedEvent first = alertCreatedCollector.poll(WAIT);
        assertThat(first).isNotNull();
        assertThat(first.transactionId()).isEqualTo(txId);

        // Duplicate processing may publish a second AlertCreatedEvent (shouldNotify() is
        // called even when saveAlert() short-circuits on an existing alert). Drain it here
        // so it does not leak into the next test's negative assertion window.
        alertCreatedCollector.poll(NEGATIVE_WAIT);

        // DB garantisi: UNIQUE transaction_id → tek satır
        Optional<Alert> savedAlert = alertRepository.findByTransactionId(txId);
        assertThat(savedAlert).isPresent();
        assertThat(savedAlert.get().getTransactionId()).isEqualTo(txId);
        assertThat(savedAlert.get().getFraudScore()).isEqualTo(80);
    }

    @Test
    @DisplayName("Alert persist edilen event — DB'deki tüm alanlar doğru set edilmeli")
    void whenEventPersisted_dbFieldsAreCorrect() throws Exception {
        String txId = "e2e-fields-" + UUID.randomUUID();
        ScoredTransactionEvent event = new ScoredTransactionEvent(
                txId, "idem-001", "user-field-check",
                java.math.BigDecimal.valueOf(15000), "USD", "RU", "fp-check",
                java.time.Instant.now(), 150, 100, RiskLevel.CRITICAL,
                List.of("HIGH_AMOUNT", "BLACKLISTED_COUNTRY"),
                java.time.Instant.now()
        );

        publishScored(event);

        AlertCreatedEvent alertEvent = alertCreatedCollector.poll(WAIT);
        assertThat(alertEvent).isNotNull();

        Optional<Alert> saved = alertRepository.findByTransactionId(txId);
        assertThat(saved).isPresent();

        Alert alert = saved.get();
        assertThat(alert.getUserId()).isEqualTo("user-field-check");
        assertThat(alert.getFraudScore()).isEqualTo(100);
        assertThat(alert.getRiskLevel()).isEqualTo("CRITICAL");
        assertThat(alert.getStatus()).isEqualTo("OPEN");
        assertThat(alert.getTriggeredRules()).contains("HIGH_AMOUNT");
        assertThat(alert.getTriggeredRules()).contains("BLACKLISTED_COUNTRY");
        assertThat(alert.getCreatedAt()).isNotNull();
        assertThat(alert.getId()).isNotNull();
    }
}
