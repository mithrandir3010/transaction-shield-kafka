package com.transactionshield.producer.controller;

import com.transactionshield.common.dto.TransactionRequest;
import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.producer.service.TransactionProducerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transactions")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    private final TransactionProducerService producerService;

    /**
     * Accepts a financial transaction and publishes it to Kafka.
     *
     * HTTP 202 Accepted — event published, async processing will follow.
     * HTTP 409 Conflict — idempotencyKey already seen within the TTL window.
     * HTTP 400 Bad Request — request payload failed bean validation.
     * HTTP 503 Service Unavailable — Kafka broker unreachable.
     */
    @PostMapping
    public ResponseEntity<TransactionResponse> submit(@Valid @RequestBody TransactionRequest request) {
        log.info("Received transaction request — idempotencyKey={} userId={} amount={} {}",
                request.idempotencyKey(), request.userId(), request.amount(), request.currency());

        TransactionResponse response = producerService.submit(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
