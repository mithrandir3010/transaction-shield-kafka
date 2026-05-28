package com.transactionshield.alert.service;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.repository.AlertRepository;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("AlertService — iş mantığı unit testleri")
class AlertServiceUnitTest {

    @Mock AlertRepository alertRepository;
    @Spy  MeterRegistry meterRegistry = new SimpleMeterRegistry();
    @InjectMocks AlertService alertService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(alertService, "notifyRiskLevels", Set.of("HIGH", "CRITICAL"));
    }

    @Test
    @DisplayName("Yeni event — alert DB'ye persist edilir")
    void saveAlert_newEvent_persistsAlert() {
        ScoredTransactionEvent event = scoredEvent("tx-new", RiskLevel.HIGH, 80);
        given(alertRepository.findByTransactionId("tx-new")).willReturn(Optional.empty());

        Alert saved = Alert.builder()
                .id(UUID.randomUUID()).transactionId("tx-new")
                .userId("user-1").fraudScore(80).riskLevel("HIGH")
                .triggeredRules("HIGH_AMOUNT").status("OPEN")
                .createdAt(Instant.now()).updatedAt(Instant.now()).build();
        given(alertRepository.save(any())).willReturn(saved);

        Alert result = alertService.saveAlert(event);

        ArgumentCaptor<Alert> captor = ArgumentCaptor.forClass(Alert.class);
        verify(alertRepository).save(captor.capture());
        Alert persisted = captor.getValue();

        assertThat(persisted.getTransactionId()).isEqualTo("tx-new");
        assertThat(persisted.getUserId()).isEqualTo("user-1");
        assertThat(persisted.getFraudScore()).isEqualTo(80);
        assertThat(persisted.getRiskLevel()).isEqualTo("HIGH");
        assertThat(persisted.getStatus()).isEqualTo("OPEN");
        assertThat(persisted.getTriggeredRules()).contains("HIGH_AMOUNT");
        assertThat(result.getTransactionId()).isEqualTo("tx-new");
    }

    @Test
    @DisplayName("Duplicate event (transactionId zaten mevcut) — mevcut alert döner, save çağrılmaz")
    void saveAlert_duplicateTransactionId_returnsExistingWithoutSave() {
        ScoredTransactionEvent event = scoredEvent("tx-dup", RiskLevel.CRITICAL, 100);
        Alert existing = Alert.builder()
                .id(UUID.randomUUID()).transactionId("tx-dup")
                .userId("user-1").fraudScore(100).riskLevel("CRITICAL")
                .status("OPEN").createdAt(Instant.now()).updatedAt(Instant.now()).build();
        given(alertRepository.findByTransactionId("tx-dup")).willReturn(Optional.of(existing));

        Alert result = alertService.saveAlert(event);

        verify(alertRepository, never()).save(any());
        assertThat(result.getTransactionId()).isEqualTo("tx-dup");
        assertThat(result.getId()).isEqualTo(existing.getId());
    }

    @Test
    @DisplayName("shouldNotify() — HIGH risk → true")
    void shouldNotify_highRisk_returnsTrue() {
        assertThat(alertService.shouldNotify(scoredEvent("tx-h", RiskLevel.HIGH, 70))).isTrue();
    }

    @Test
    @DisplayName("shouldNotify() — CRITICAL risk → true")
    void shouldNotify_criticalRisk_returnsTrue() {
        assertThat(alertService.shouldNotify(scoredEvent("tx-c", RiskLevel.CRITICAL, 100))).isTrue();
    }

    @Test
    @DisplayName("shouldNotify() — MEDIUM risk → false")
    void shouldNotify_mediumRisk_returnsFalse() {
        assertThat(alertService.shouldNotify(scoredEvent("tx-m", RiskLevel.MEDIUM, 50))).isFalse();
    }

    @Test
    @DisplayName("shouldNotify() — LOW risk → false")
    void shouldNotify_lowRisk_returnsFalse() {
        assertThat(alertService.shouldNotify(scoredEvent("tx-l", RiskLevel.LOW, 0))).isFalse();
    }

    @Test
    @DisplayName("triggeredRules listesi virgülle ayrılmış string olarak persist edilir")
    void saveAlert_triggeredRulesStoredAsCommaSeparated() {
        ScoredTransactionEvent event = new ScoredTransactionEvent(
                "tx-rules", "idem-1", "user-1",
                BigDecimal.valueOf(15000), "USD", "RU", null,
                Instant.now(), 150, 100, RiskLevel.CRITICAL,
                List.of("HIGH_AMOUNT", "BLACKLISTED_COUNTRY"),
                Instant.now()
        );
        given(alertRepository.findByTransactionId("tx-rules")).willReturn(Optional.empty());
        given(alertRepository.save(any())).willAnswer(inv -> inv.getArgument(0));

        alertService.saveAlert(event);

        ArgumentCaptor<Alert> captor = ArgumentCaptor.forClass(Alert.class);
        verify(alertRepository).save(captor.capture());
        assertThat(captor.getValue().getTriggeredRules())
                .contains("HIGH_AMOUNT")
                .contains("BLACKLISTED_COUNTRY");
    }

    private ScoredTransactionEvent scoredEvent(String txId, RiskLevel level, int score) {
        return new ScoredTransactionEvent(
                txId, "idem-1", "user-1",
                BigDecimal.valueOf(100), "USD", "US", null,
                Instant.now(), score, score, level,
                List.of("HIGH_AMOUNT"),
                Instant.now()
        );
    }
}
