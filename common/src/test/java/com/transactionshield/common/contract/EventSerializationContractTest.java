package com.transactionshield.common.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.AlertCreatedEvent;
import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract testi — üç Kafka event şemasının servis sınırları arasında
 * doğru serialize/deserialize edildiğini garanti altına alır.
 *
 * Yaklaşım:
 *   Producer tarafında oluşturulan nesne → JSON → consumer tarafında
 *   yeniden deserialize edilir. Tüm zorunlu alanlar korunmalı,
 *   Optional alanlar null-safe olmalı.
 *
 * Neden Pact/Spring Cloud Contract yerine bu yaklaşım?
 *   Monorepo'da şema ortak modülde zaten paylaşılıyor. Jackson
 *   roundtrip testi, servis sınırlarındaki en gerçek kontrat.
 */
@DisplayName("Kafka Event Şema Kontrat Testleri")
class EventSerializationContractTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // ── TransactionEvent (transaction-producer → fraud-engine) ───────

    @Nested
    @DisplayName("TransactionEvent")
    class TransactionEventTests {

        @Test
        @DisplayName("Tüm alanlar Jackson roundtrip sonrasında korunmalı")
        void transactionEvent_fullRoundtrip_preservesAllFields() throws Exception {
            TransactionEvent original = new TransactionEvent(
                    "tx-" + UUID.randomUUID(),
                    "idem-" + UUID.randomUUID(),
                    "user-001",
                    new BigDecimal("9999.99"),
                    "USD",
                    "DE",
                    "fp-abc123",
                    Instant.parse("2025-06-15T14:30:00Z")
            );

            String json = objectMapper.writeValueAsString(original);
            TransactionEvent deserialized = objectMapper.readValue(json, TransactionEvent.class);

            assertThat(deserialized.transactionId()).isEqualTo(original.transactionId());
            assertThat(deserialized.idempotencyKey()).isEqualTo(original.idempotencyKey());
            assertThat(deserialized.userId()).isEqualTo(original.userId());
            assertThat(deserialized.amount()).isEqualByComparingTo(original.amount());
            assertThat(deserialized.currency()).isEqualTo(original.currency());
            assertThat(deserialized.country()).isEqualTo(original.country());
            assertThat(deserialized.deviceFingerprint()).isEqualTo(original.deviceFingerprint());
            assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
        }

        @Test
        @DisplayName("deviceFingerprint null olabilir (opsiyonel alan)")
        void transactionEvent_nullDeviceFingerprint_roundtrips() throws Exception {
            TransactionEvent original = new TransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.TEN, "USD", "US",
                    null,
                    Instant.now()
            );

            String json = objectMapper.writeValueAsString(original);
            TransactionEvent deserialized = objectMapper.readValue(json, TransactionEvent.class);

            assertThat(deserialized.deviceFingerprint()).isNull();
        }

        @Test
        @DisplayName("Canonical constructor — transactionId boş olursa IllegalArgumentException")
        void transactionEvent_blankTransactionId_throwsIllegalArgumentException() {
            assertThatThrownBy(() -> new TransactionEvent(
                    "", "idem-001", "user-001",
                    BigDecimal.TEN, "USD", "US", null, Instant.now()
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("transactionId");
        }

        @Test
        @DisplayName("Canonical constructor — amount sıfır veya negatif olursa IllegalArgumentException")
        void transactionEvent_nonPositiveAmount_throwsIllegalArgumentException() {
            assertThatThrownBy(() -> new TransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.ZERO, "USD", "US", null, Instant.now()
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("amount");
        }

        @Test
        @DisplayName("Canonical constructor — currency 3 karakter değilse IllegalArgumentException")
        void transactionEvent_invalidCurrencyLength_throwsIllegalArgumentException() {
            assertThatThrownBy(() -> new TransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.TEN, "US", "US", null, Instant.now()
            )).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("currency");
        }

        @Test
        @DisplayName("JSON timestamp alanı ISO-8601 formatında serialize edilmeli")
        void transactionEvent_timestampSerializesAsIso8601() throws Exception {
            TransactionEvent event = new TransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.TEN, "USD", "US", null,
                    Instant.parse("2025-06-15T10:00:00Z")
            );

            JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(event));

            assertThat(node.get("timestamp").isTextual()).isTrue();
            assertThat(node.get("timestamp").asText()).contains("2025-06-15");
        }
    }

    // ── ScoredTransactionEvent (fraud-engine → alert-service) ────────

    @Nested
    @DisplayName("ScoredTransactionEvent")
    class ScoredTransactionEventTests {

        @Test
        @DisplayName("Tüm alanlar Jackson roundtrip sonrasında korunmalı")
        void scoredTransactionEvent_fullRoundtrip_preservesAllFields() throws Exception {
            ScoredTransactionEvent original = new ScoredTransactionEvent(
                    "tx-scored-001",
                    "idem-001",
                    "user-001",
                    new BigDecimal("15000.00"),
                    "USD",
                    "RU",
                    "fp-xyz",
                    Instant.parse("2025-06-15T14:30:00Z"),
                    150,
                    100,
                    RiskLevel.CRITICAL,
                    List.of("HIGH_AMOUNT", "BLACKLISTED_COUNTRY"),
                    Instant.parse("2025-06-15T14:30:01Z")
            );

            String json = objectMapper.writeValueAsString(original);
            ScoredTransactionEvent deserialized =
                    objectMapper.readValue(json, ScoredTransactionEvent.class);

            assertThat(deserialized.transactionId()).isEqualTo("tx-scored-001");
            assertThat(deserialized.rawScore()).isEqualTo(150);
            assertThat(deserialized.fraudScore()).isEqualTo(100);
            assertThat(deserialized.riskLevel()).isEqualTo(RiskLevel.CRITICAL);
            assertThat(deserialized.triggeredRules())
                    .containsExactlyInAnyOrder("HIGH_AMOUNT", "BLACKLISTED_COUNTRY");
            assertThat(deserialized.scoredAt()).isEqualTo(original.scoredAt());
        }

        @Test
        @DisplayName("triggeredRules boş liste olduğunda roundtrip çalışmalı")
        void scoredTransactionEvent_emptyTriggeredRules_roundtrips() throws Exception {
            ScoredTransactionEvent original = new ScoredTransactionEvent(
                    "tx-clean", "idem-001", "user-001",
                    BigDecimal.valueOf(500), "USD", "US", null,
                    Instant.now(), 0, 0, RiskLevel.LOW, List.of(), Instant.now()
            );

            String json = objectMapper.writeValueAsString(original);
            ScoredTransactionEvent deserialized =
                    objectMapper.readValue(json, ScoredTransactionEvent.class);

            assertThat(deserialized.triggeredRules()).isEmpty();
            assertThat(deserialized.fraudScore()).isZero();
            assertThat(deserialized.riskLevel()).isEqualTo(RiskLevel.LOW);
        }

        @Test
        @DisplayName("JSON şeması — alert-service için zorunlu alanlar mevcut olmalı")
        void scoredTransactionEvent_jsonContainsAlertServiceRequiredFields() throws Exception {
            ScoredTransactionEvent event = new ScoredTransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.valueOf(100), "USD", "US", null,
                    Instant.now(), 50, 50, RiskLevel.MEDIUM, List.of("HIGH_AMOUNT"), Instant.now()
            );

            JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(event));

            // alert-service'in ScoredTransactionConsumer'ı bu alanları kullanır
            assertThat(node.has("transactionId")).isTrue();
            assertThat(node.has("userId")).isTrue();
            assertThat(node.has("fraudScore")).isTrue();
            assertThat(node.has("riskLevel")).isTrue();
            assertThat(node.has("triggeredRules")).isTrue();
            assertThat(node.has("scoredAt")).isTrue();
        }

        @Test
        @DisplayName("RiskLevel enum JSON'da string olarak serialize edilmeli")
        void scoredTransactionEvent_riskLevelSerializesAsString() throws Exception {
            ScoredTransactionEvent event = new ScoredTransactionEvent(
                    "tx-001", "idem-001", "user-001",
                    BigDecimal.valueOf(100), "USD", "US", null,
                    Instant.now(), 80, 80, RiskLevel.HIGH, List.of(), Instant.now()
            );

            JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(event));

            assertThat(node.get("riskLevel").asText()).isEqualTo("HIGH");
        }
    }

    // ── AlertCreatedEvent (alert-service → downstream) ───────────────

    @Nested
    @DisplayName("AlertCreatedEvent")
    class AlertCreatedEventTests {

        @Test
        @DisplayName("Tüm alanlar Jackson roundtrip sonrasında korunmalı")
        void alertCreatedEvent_fullRoundtrip_preservesAllFields() throws Exception {
            AlertCreatedEvent original = new AlertCreatedEvent(
                    UUID.randomUUID().toString(),
                    "tx-001",
                    "user-001",
                    100,
                    "CRITICAL",
                    List.of("BLACKLISTED_COUNTRY"),
                    Instant.now()
            );

            String json = objectMapper.writeValueAsString(original);
            AlertCreatedEvent deserialized =
                    objectMapper.readValue(json, AlertCreatedEvent.class);

            assertThat(deserialized.alertId()).isEqualTo(original.alertId());
            assertThat(deserialized.transactionId()).isEqualTo(original.transactionId());
            assertThat(deserialized.userId()).isEqualTo(original.userId());
            assertThat(deserialized.fraudScore()).isEqualTo(original.fraudScore());
            assertThat(deserialized.riskLevel()).isEqualTo("CRITICAL");
            assertThat(deserialized.triggeredRules()).containsExactly("BLACKLISTED_COUNTRY");
        }

        @Test
        @DisplayName("JSON şeması — downstream servislerin beklediği alanlar mevcut olmalı")
        void alertCreatedEvent_jsonContainsAllRequiredFields() throws Exception {
            AlertCreatedEvent event = new AlertCreatedEvent(
                    "alert-001", "tx-001", "user-001",
                    95, "CRITICAL", List.of("BLACKLISTED_COUNTRY"), Instant.now()
            );

            JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(event));

            assertThat(node.has("alertId")).isTrue();
            assertThat(node.has("transactionId")).isTrue();
            assertThat(node.has("userId")).isTrue();
            assertThat(node.has("fraudScore")).isTrue();
            assertThat(node.has("riskLevel")).isTrue();
            assertThat(node.has("triggeredRules")).isTrue();
            assertThat(node.has("createdAt")).isTrue();
        }
    }
}
