package com.transactionshield.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Append-only audit record written after every fraud scoring.
 *
 * Captures the variant (STABLE / EXPERIMENT) so analysts can compare
 * scoring outcomes across rule set versions without reprocessing events.
 */
@Entity
@Table(name = "scoring_audit_log")
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ScoringAuditLog {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "transaction_id", nullable = false, length = 64)
    private String transactionId;

    @Column(nullable = false, length = 32)
    private String variant;

    @Column(name = "raw_score", nullable = false)
    private int rawScore;

    @Column(name = "fraud_score", nullable = false)
    private int fraudScore;

    @Column(name = "risk_level", nullable = false, length = 32)
    private String riskLevel;

    @Column(name = "triggered_rules", columnDefinition = "TEXT")
    private String triggeredRules;

    @Column(name = "evaluated_at", nullable = false)
    private Instant evaluatedAt;
}
