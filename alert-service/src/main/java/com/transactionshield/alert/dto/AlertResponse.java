package com.transactionshield.alert.dto;

import com.transactionshield.alert.entity.Alert;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public record AlertResponse(
        String id,
        String transactionId,
        String userId,
        Integer fraudScore,
        String riskLevel,
        List<String> triggeredRules,
        String status,
        Instant createdAt
) {
    public static AlertResponse from(Alert alert) {
        List<String> rules = (alert.getTriggeredRules() != null && !alert.getTriggeredRules().isBlank())
                ? Arrays.asList(alert.getTriggeredRules().split(","))
                : List.of();
        return new AlertResponse(
                alert.getId().toString(),
                alert.getTransactionId(),
                alert.getUserId(),
                alert.getFraudScore(),
                alert.getRiskLevel(),
                rules,
                alert.getStatus(),
                alert.getCreatedAt()
        );
    }
}
