package com.transactionshield.common.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Immutable Kafka event published to the `transactions.raw` topic.
 * Java 21 record — canonical constructor enforces non-null invariants.
 */
public record TransactionEvent(
        String transactionId,
        String idempotencyKey,
        String userId,
        BigDecimal amount,
        String currency,
        String country,
        String deviceFingerprint,
        Instant timestamp
) {
    public TransactionEvent {
        if (transactionId  == null || transactionId.isBlank())  throw new IllegalArgumentException("transactionId must not be blank");
        if (idempotencyKey == null || idempotencyKey.isBlank()) throw new IllegalArgumentException("idempotencyKey must not be blank");
        if (userId         == null || userId.isBlank())         throw new IllegalArgumentException("userId must not be blank");
        if (amount         == null || amount.signum() <= 0)     throw new IllegalArgumentException("amount must be positive");
        if (currency       == null || currency.length() != 3)   throw new IllegalArgumentException("currency must be a 3-letter ISO code");
        if (country        == null || country.isBlank())        throw new IllegalArgumentException("country must not be blank");
        if (timestamp      == null)                             throw new IllegalArgumentException("timestamp must not be null");
    }
}
