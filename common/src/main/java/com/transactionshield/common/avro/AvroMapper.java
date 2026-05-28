package com.transactionshield.common.avro;

import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.AlertCreatedEvent;
import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Converts between domain Java records (internal model) and Avro-generated classes (Kafka wire format).
 *
 * Domain records drive all service business logic.
 * Avro classes cross the Kafka boundary — producers call toAvro(), consumers call fromAvro().
 * This is the only layer where the two representations touch.
 */
public final class AvroMapper {

    private AvroMapper() {}

    // ── TransactionEvent ────────────────────────────────────────────────

    public static com.transactionshield.avro.TransactionEvent toAvro(TransactionEvent e) {
        return com.transactionshield.avro.TransactionEvent.newBuilder()
                .setTransactionId(e.transactionId())
                .setIdempotencyKey(e.idempotencyKey())
                .setUserId(e.userId())
                .setAmount(e.amount().toPlainString())
                .setCurrency(e.currency())
                .setCountry(e.country())
                .setDeviceFingerprint(e.deviceFingerprint())
                .setTimestampMillis(e.timestamp().toEpochMilli())
                .build();
    }

    public static TransactionEvent fromAvro(com.transactionshield.avro.TransactionEvent a) {
        return new TransactionEvent(
                a.getTransactionId(),
                a.getIdempotencyKey(),
                a.getUserId(),
                new BigDecimal(a.getAmount()),
                a.getCurrency(),
                a.getCountry(),
                a.getDeviceFingerprint(),
                Instant.ofEpochMilli(a.getTimestampMillis())
        );
    }

    // ── ScoredTransactionEvent ──────────────────────────────────────────

    public static com.transactionshield.avro.ScoredTransactionEvent toAvro(ScoredTransactionEvent e) {
        return com.transactionshield.avro.ScoredTransactionEvent.newBuilder()
                .setTransactionId(e.transactionId())
                .setIdempotencyKey(e.idempotencyKey())
                .setUserId(e.userId())
                .setAmount(e.amount().toPlainString())
                .setCurrency(e.currency())
                .setCountry(e.country())
                .setDeviceFingerprint(e.deviceFingerprint())
                .setOriginalTimestampMillis(e.originalTimestamp().toEpochMilli())
                .setRawScore(e.rawScore())
                .setFraudScore(e.fraudScore())
                .setRiskLevel(com.transactionshield.avro.RiskLevel.valueOf(e.riskLevel().name()))
                .setTriggeredRules(e.triggeredRules())
                .setScoredAtMillis(e.scoredAt().toEpochMilli())
                .build();
    }

    public static ScoredTransactionEvent fromAvro(com.transactionshield.avro.ScoredTransactionEvent a) {
        return new ScoredTransactionEvent(
                a.getTransactionId(),
                a.getIdempotencyKey(),
                a.getUserId(),
                new BigDecimal(a.getAmount()),
                a.getCurrency(),
                a.getCountry(),
                a.getDeviceFingerprint(),
                Instant.ofEpochMilli(a.getOriginalTimestampMillis()),
                a.getRawScore(),
                a.getFraudScore(),
                RiskLevel.valueOf(a.getRiskLevel().name()),
                List.copyOf(a.getTriggeredRules()),
                Instant.ofEpochMilli(a.getScoredAtMillis())
        );
    }

    // ── AlertCreatedEvent ───────────────────────────────────────────────

    public static com.transactionshield.avro.AlertCreatedEvent toAvro(AlertCreatedEvent e) {
        return com.transactionshield.avro.AlertCreatedEvent.newBuilder()
                .setAlertId(e.alertId())
                .setTransactionId(e.transactionId())
                .setUserId(e.userId())
                .setFraudScore(e.fraudScore())
                .setRiskLevel(e.riskLevel())
                .setTriggeredRules(e.triggeredRules())
                .setCreatedAtMillis(e.createdAt().toEpochMilli())
                .build();
    }

    public static AlertCreatedEvent fromAvro(com.transactionshield.avro.AlertCreatedEvent a) {
        return new AlertCreatedEvent(
                a.getAlertId(),
                a.getTransactionId(),
                a.getUserId(),
                a.getFraudScore(),
                a.getRiskLevel(),
                List.copyOf(a.getTriggeredRules()),
                Instant.ofEpochMilli(a.getCreatedAtMillis())
        );
    }
}
