package com.transactionshield.common.dto;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;

/**
 * Inbound REST payload for POST /api/v1/transactions.
 * Bean Validation runs on record components via @Valid in the controller.
 */
public record TransactionRequest(

        @NotBlank(message = "idempotencyKey is required")
        @Size(max = 128, message = "idempotencyKey must not exceed 128 characters")
        String idempotencyKey,

        @NotBlank(message = "userId is required")
        String userId,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be greater than zero")
        @Digits(integer = 15, fraction = 4, message = "amount must have at most 15 integer and 4 fraction digits")
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        @Size(min = 3, max = 3, message = "currency must be a 3-letter ISO 4217 code")
        String currency,

        @NotBlank(message = "country is required")
        @Size(min = 2, max = 2, message = "country must be a 2-letter ISO 3166-1 alpha-2 code")
        String country,

        // Optional: enriched later by the fraud engine
        String deviceFingerprint
) {}
