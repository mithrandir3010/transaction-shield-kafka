package com.transactionshield.common.event;

import java.time.Instant;
import java.util.List;

/**
 * Kafka event published to `alerts.created` when the alert-service
 * determines a transaction is HIGH or CRITICAL risk.
 *
 * Downstream services (email, SMS, push notification) consume this
 * topic independently — alert-service doesn't know or care who they are.
 */
public record AlertCreatedEvent(
        String alertId,
        String transactionId,
        String userId,
        int fraudScore,
        String riskLevel,           // HIGH | CRITICAL
        List<String> triggeredRules,
        Instant createdAt
) {}
