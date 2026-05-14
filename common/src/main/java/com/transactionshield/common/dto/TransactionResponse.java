package com.transactionshield.common.dto;

import java.time.Instant;

/**
 * Response body returned after a transaction is accepted and published to Kafka.
 */
public record TransactionResponse(
        String transactionId,
        String idempotencyKey,
        String status,
        Instant acceptedAt
) {}
