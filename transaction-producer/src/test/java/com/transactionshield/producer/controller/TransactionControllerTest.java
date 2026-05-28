package com.transactionshield.producer.controller;

import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.producer.exception.DuplicateTransactionException;
import com.transactionshield.producer.exception.KafkaPublishException;
import com.transactionshield.producer.service.TransactionProducerService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(TransactionController.class)
@DisplayName("TransactionController — HTTP katmanı testleri")
class TransactionControllerTest {

    @Autowired MockMvc mockMvc;
    @MockBean TransactionProducerService producerService;

    private static final String URL = "/api/v1/transactions";

    @Test
    @DisplayName("Geçerli istek → 202 Accepted + transactionId döner")
    void submit_validRequest_returns202() throws Exception {
        given(producerService.submit(any())).willReturn(
                new TransactionResponse("tx-001", "key-001", "ACCEPTED", Instant.now()));

        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("key-001")))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.transactionId").value("tx-001"))
                .andExpect(jsonPath("$.idempotencyKey").value("key-001"));
    }

    @Test
    @DisplayName("Duplicate idempotencyKey → 409 Conflict")
    void submit_duplicateKey_returns409() throws Exception {
        given(producerService.submit(any()))
                .willThrow(new DuplicateTransactionException("key-dup"));

        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("key-dup")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    @DisplayName("Kafka broker erişilemiyor → 503 Service Unavailable")
    void submit_kafkaFailure_returns503() throws Exception {
        given(producerService.submit(any()))
                .willThrow(new KafkaPublishException("Kafka down", new RuntimeException()));

        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validBody("key-kafka")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    @DisplayName("idempotencyKey eksik → 400 Bad Request")
    void submit_missingIdempotencyKey_returns400() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "userId": "user-1",
                                    "amount": 100.00,
                                    "currency": "USD",
                                    "country": "US"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("amount negatif → 400 Bad Request")
    void submit_negativeAmount_returns400() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "idempotencyKey": "key-1",
                                    "userId": "user-1",
                                    "amount": -50.00,
                                    "currency": "USD",
                                    "country": "US"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("currency 3 karakterden farklı → 400 Bad Request")
    void submit_invalidCurrency_returns400() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "idempotencyKey": "key-1",
                                    "userId": "user-1",
                                    "amount": 100.00,
                                    "currency": "US",
                                    "country": "US"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("country 2 karakterden farklı → 400 Bad Request")
    void submit_invalidCountry_returns400() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "idempotencyKey": "key-1",
                                    "userId": "user-1",
                                    "amount": 100.00,
                                    "currency": "USD",
                                    "country": "USA"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("deviceFingerprint opsiyonel — null olunca geçerli istek")
    void submit_nullDeviceFingerprint_isValid() throws Exception {
        given(producerService.submit(any())).willReturn(
                new TransactionResponse("tx-002", "key-002", "ACCEPTED", Instant.now()));

        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "idempotencyKey": "key-002",
                                    "userId": "user-1",
                                    "amount": 100.00,
                                    "currency": "USD",
                                    "country": "US"
                                }
                                """))
                .andExpect(status().isAccepted());
    }

    private String validBody(String key) {
        return """
                {
                    "idempotencyKey": "%s",
                    "userId": "user-001",
                    "amount": 500.00,
                    "currency": "USD",
                    "country": "US",
                    "deviceFingerprint": "fp-test"
                }
                """.formatted(key);
    }
}
