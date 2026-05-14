package com.transactionshield.producer.service;

import com.transactionshield.common.dto.TransactionRequest;
import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.producer.exception.DuplicateTransactionException;
import com.transactionshield.producer.exception.KafkaPublishException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionProducerService {

    private final IdempotencyService idempotencyService;
    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Value("${app.kafka.transactions-raw-topic}")
    private String transactionsRawTopic;

    @Value("${app.kafka.publish-timeout-seconds:5}")
    private long publishTimeoutSeconds;

    /**
     * Full request lifecycle:
     *
     *   [1] Idempotency check  — Redis SET NX
     *   [2] Build event        — assign transactionId + timestamp
     *   [3] Kafka publish      — synchronous send with timeout
     *   [4] On failure         — compensate: delete Redis key → client can retry
     *
     * Why synchronous (.get()) instead of async callback?
     * We need to know publish outcome BEFORE returning the HTTP response.
     * Returning 202 without confirmation would be a false acknowledgement.
     */
    public TransactionResponse submit(TransactionRequest request) {
        String idempotencyKey = request.idempotencyKey();

        // ── Step 1: Idempotency guard ─────────────────────────────────
        if (!idempotencyService.tryAcquire(idempotencyKey)) {
            throw new DuplicateTransactionException(idempotencyKey);
        }

        // ── Step 2: Build immutable event ─────────────────────────────
        TransactionEvent event = new TransactionEvent(
                UUID.randomUUID().toString(),
                idempotencyKey,
                request.userId(),
                request.amount(),
                request.currency(),
                request.country(),
                request.deviceFingerprint(),
                Instant.now()
        );

        // ── Step 3: Publish to Kafka ──────────────────────────────────
        try {
            kafkaTemplate.send(transactionsRawTopic, event.transactionId(), event)
                    .get(publishTimeoutSeconds, TimeUnit.SECONDS);

            log.info("Transaction published — transactionId={} idempotencyKey={} topic={}",
                    event.transactionId(), idempotencyKey, transactionsRawTopic);

        } catch (Exception ex) {
            // ── Step 4: Compensate — release Redis lock on failure ────
            // The client may safely retry with the same idempotencyKey.
            idempotencyService.release(idempotencyKey);
            throw new KafkaPublishException(
                    "Failed to publish transaction " + event.transactionId() + " to Kafka", ex);
        }

        return new TransactionResponse(
                event.transactionId(),
                idempotencyKey,
                "ACCEPTED",
                event.timestamp()
        );
    }
}
