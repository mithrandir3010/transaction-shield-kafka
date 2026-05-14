package com.transactionshield.common.event;

import com.transactionshield.common.enums.RiskLevel;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Kafka event published to `transactions.scored` after the fraud engine
 * evaluates a TransactionEvent. Consumed by the alert-service.
 *
 * Carries the original transaction fields plus scoring metadata so
 * downstream consumers don't need to look up the original event.
 */
public record ScoredTransactionEvent(

        // ── Original transaction fields ──────────────────────────
        String transactionId,
        String idempotencyKey,
        String userId,
        BigDecimal amount,
        String currency,
        String country,
        String deviceFingerprint,
        Instant originalTimestamp,

        // ── Scoring results ───────────────────────────────────────
        int rawScore,          // sum of all triggered rule scores before capping
        int fraudScore,        // capped at 100; stored in DB and used for alerts
        RiskLevel riskLevel,
        List<String> triggeredRules,  // rule codes of triggered rules
        Instant scoredAt
) {}
