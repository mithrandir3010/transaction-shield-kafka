package com.transactionshield.producer.service;

import com.transactionshield.common.dto.TransactionRequest;
import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.producer.exception.DuplicateTransactionException;
import com.transactionshield.producer.exception.KafkaPublishException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionProducerService — işlem gönderme lifecycle")
class TransactionProducerServiceTest {

    @Mock IdempotencyService idempotencyService;
    @Mock KafkaTemplate<String, com.transactionshield.avro.TransactionEvent> kafkaTemplate;
    @Spy  MeterRegistry meterRegistry = new SimpleMeterRegistry();
    @InjectMocks TransactionProducerService producerService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(producerService, "transactionsRawTopic", "transactions.raw");
        ReflectionTestUtils.setField(producerService, "publishTimeoutSeconds", 5L);
    }

    @Test
    @DisplayName("Happy path: idempotency geçer, Kafka'ya publish edilir, 202 döner")
    void submit_happyPath_returnsAccepted() {
        TransactionRequest request = validRequest("key-happy");
        given(idempotencyService.tryAcquire("key-happy")).willReturn(true);
        given(kafkaTemplate.send(eq("transactions.raw"), any(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        TransactionResponse response = producerService.submit(request);

        assertThat(response.status()).isEqualTo("ACCEPTED");
        assertThat(response.idempotencyKey()).isEqualTo("key-happy");
        assertThat(response.transactionId()).isNotBlank();
        assertThat(response.acceptedAt()).isNotNull();
        verify(idempotencyService, never()).release(any());
    }

    @Test
    @DisplayName("Duplicate idempotencyKey → DuplicateTransactionException, Kafka'ya yazılmaz")
    void submit_duplicateKey_throwsDuplicateException() {
        TransactionRequest request = validRequest("key-dup");
        given(idempotencyService.tryAcquire("key-dup")).willReturn(false);

        assertThatThrownBy(() -> producerService.submit(request))
                .isInstanceOf(DuplicateTransactionException.class)
                .hasMessageContaining("key-dup");

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("Kafka publish başarısız → idempotency lock serbest bırakılır, KafkaPublishException")
    void submit_kafkaPublishFails_releasesLockAndThrows() {
        TransactionRequest request = validRequest("key-kafka-fail");
        given(idempotencyService.tryAcquire("key-kafka-fail")).willReturn(true);

        CompletableFuture<SendResult<String, com.transactionshield.avro.TransactionEvent>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unreachable"));
        given(kafkaTemplate.send(any(), any(), any())).willReturn(failedFuture);

        assertThatThrownBy(() -> producerService.submit(request))
                .isInstanceOf(KafkaPublishException.class);

        verify(idempotencyService).release("key-kafka-fail");
    }

    @Test
    @DisplayName("Kafka timeout → idempotency lock serbest bırakılır")
    void submit_kafkaTimeout_releasesLock() {
        TransactionRequest request = validRequest("key-timeout");
        ReflectionTestUtils.setField(producerService, "publishTimeoutSeconds", 0L); // immediate timeout
        given(idempotencyService.tryAcquire("key-timeout")).willReturn(true);

        // Future hiçbir zaman tamamlanmıyor — timeout tetiklenir
        given(kafkaTemplate.send(any(), any(), any()))
                .willReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> producerService.submit(request))
                .isInstanceOf(KafkaPublishException.class);

        verify(idempotencyService).release("key-timeout");
    }

    @Test
    @DisplayName("Publish edilen Kafka event'i istekle uyumlu olmalı")
    void submit_publishedEventMatchesRequest() {
        TransactionRequest request = new TransactionRequest(
                "key-check", "user-99",
                new BigDecimal("12345.67"), "EUR", "DE", "fp-device"
        );
        given(idempotencyService.tryAcquire(any())).willReturn(true);
        given(kafkaTemplate.send(any(), any(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producerService.submit(request);

        verify(kafkaTemplate).send(
                eq("transactions.raw"),
                any(String.class),
                argThat(event ->
                        event.getUserId().equals("user-99") &&
                        new BigDecimal(event.getAmount()).compareTo(new BigDecimal("12345.67")) == 0 &&
                        event.getCurrency().equals("EUR") &&
                        event.getCountry().equals("DE") &&
                        event.getDeviceFingerprint().equals("fp-device") &&
                        event.getTransactionId() != null &&
                        event.getTimestampMillis() > 0
                )
        );
    }

    private TransactionRequest validRequest(String idempotencyKey) {
        return new TransactionRequest(idempotencyKey, "user-001",
                BigDecimal.valueOf(500), "USD", "US", "fp-test");
    }
}
