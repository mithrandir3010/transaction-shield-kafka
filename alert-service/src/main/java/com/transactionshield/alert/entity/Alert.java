package com.transactionshield.alert.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity persisted to the `alerts` table.
 *
 * Design notes:
 *  - riskLevel stored as VARCHAR (not PostgreSQL enum) for schema flexibility.
 *  - transaction_id has a UNIQUE constraint → DB-level idempotency guard;
 *    duplicate ScoredTransactionEvents from Kafka at-least-once delivery
 *    will cause a DataIntegrityViolationException that is caught in the service.
 *  - No FK to transactions table: alert-service operates independently.
 */
@Entity
@Table(name = "alerts")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Alert {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, unique = true, length = 64)
    private String transactionId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "fraud_score", nullable = false)
    private Integer fraudScore;

    @Column(name = "risk_level", nullable = false, length = 20)
    private String riskLevel;            // LOW | MEDIUM | HIGH | CRITICAL

    @Column(name = "triggered_rules", length = 512)
    private String triggeredRules;       // comma-separated rule codes

    @Builder.Default
    @Column(name = "status", nullable = false, length = 30)
    private String status = "OPEN";

    @Column(name = "notes")
    private String notes;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
