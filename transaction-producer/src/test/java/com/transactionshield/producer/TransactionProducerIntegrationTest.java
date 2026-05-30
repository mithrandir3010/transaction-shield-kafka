package com.transactionshield.producer;

import com.transactionshield.common.dto.TransactionRequest;
import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.common.event.TransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * transaction-producer End-to-End Integration Testi
 *
 * HTTP POST → TransactionProducerService → Redis idempotency → Kafka transactions.raw
 *
 * Her test: gerçek Kafka (Testcontainers) + gerçek Redis.
 * RawEventCollector transactions.raw'ı dinleyerek publish doğrular.
 */
@DisplayName("TransactionProducer — End-to-End Integration Tests")
class TransactionProducerIntegrationTest extends AbstractProducerIntegrationTest {

    private static final String URL     = "/api/v1/transactions";
    private static final Duration WAIT  = Duration.ofSeconds(10);

    @Test
    @DisplayName("Geçerli istek → 202 Accepted + Kafka'ya event publish edilir")
    void submit_validRequest_returns202AndPublishesToKafka() throws Exception {
        TransactionRequest request = new TransactionRequest(
                "int-key-001", "user-int-001",
                new BigDecimal("750.00"), "USD", "US", "fp-int-001"
        );

        ResponseEntity<TransactionResponse> response =
                postWithAuth(URL, request, TransactionResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("ACCEPTED");
        assertThat(response.getBody().transactionId()).isNotBlank();

        // Kafka'ya gerçekten publish edildi mi?
        TransactionEvent published = collector.poll(WAIT);
        assertThat(published).as("Event Kafka'ya publish edilmeli").isNotNull();
        assertThat(published.userId()).isEqualTo("user-int-001");
        assertThat(published.amount()).isEqualByComparingTo("750.00");
        assertThat(published.currency()).isEqualTo("USD");
        assertThat(published.country()).isEqualTo("US");
        assertThat(published.idempotencyKey()).isEqualTo("int-key-001");
        assertThat(published.transactionId()).isNotBlank();
        assertThat(published.timestamp()).isNotNull();
    }

    @Test
    @DisplayName("Aynı idempotencyKey ile ikinci istek → 409 Conflict, Kafka'ya ikinci event yazılmaz")
    void submit_duplicateIdempotencyKey_returns409() throws Exception {
        String idempotencyKey = "int-dup-key-" + System.currentTimeMillis();
        TransactionRequest request = new TransactionRequest(
                idempotencyKey, "user-int-002",
                new BigDecimal("200.00"), "EUR", "DE", null
        );

        // İlk istek başarılı
        ResponseEntity<TransactionResponse> first =
                postWithAuth(URL, request, TransactionResponse.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // İlk event Kafka'ya geldi
        TransactionEvent firstEvent = collector.poll(WAIT);
        assertThat(firstEvent).isNotNull();

        // İkinci istek — aynı key
        ResponseEntity<String> second =
                postWithAuth(URL, request, String.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        // Kafka'ya ikinci event gelmemeli
        TransactionEvent secondEvent = collector.poll(Duration.ofSeconds(3));
        assertThat(secondEvent).as("Duplicate için ikinci Kafka event olmamalı").isNull();
    }

    @Test
    @DisplayName("Validation hatası: amount negatif → 400, Kafka'ya event yazılmaz")
    void submit_invalidAmount_returns400_noKafkaEvent() throws Exception {
        TransactionRequest badRequest = new TransactionRequest(
                "int-key-bad", "user-int-003",
                new BigDecimal("-100.00"), "USD", "US", null
        );

        ResponseEntity<String> response =
                postWithAuth(URL, badRequest, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        TransactionEvent event = collector.poll(Duration.ofSeconds(2));
        assertThat(event).as("Geçersiz istek için Kafka event olmamalı").isNull();
    }

    @Test
    @DisplayName("Farklı idempotencyKey'ler bağımsız işlenmeli")
    void submit_differentKeys_bothPublishedToKafka() throws Exception {
        TransactionRequest req1 = new TransactionRequest(
                "int-key-a-" + System.currentTimeMillis(), "user-a",
                BigDecimal.valueOf(100), "USD", "US", null);
        TransactionRequest req2 = new TransactionRequest(
                "int-key-b-" + System.currentTimeMillis(), "user-b",
                BigDecimal.valueOf(200), "GBP", "GB", null);

        postWithAuth(URL, req1, TransactionResponse.class);
        postWithAuth(URL, req2, TransactionResponse.class);

        TransactionEvent event1 = collector.poll(WAIT);
        TransactionEvent event2 = collector.poll(WAIT);

        assertThat(event1).isNotNull();
        assertThat(event2).isNotNull();
        assertThat(event1.transactionId()).isNotEqualTo(event2.transactionId());
    }
}
