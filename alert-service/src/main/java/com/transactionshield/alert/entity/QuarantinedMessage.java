package com.transactionshield.alert.entity;

import com.transactionshield.common.dlq.ErrorCategory;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Persistent record of a Kafka message that could not be recovered.
 *
 * A message is quarantined when:
 *   - Its error category is FATAL (schema/deserialization failure)
 *   - It has exceeded the maximum replay attempt threshold
 *
 * Lifecycle: OPEN → RESOLVED or DISCARDED (via operator action).
 */
@Entity
@Table(
        name = "quarantined_messages",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_quarantine_txid_topic",
                columnNames = {"transaction_id", "original_topic"}
        )
)
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class QuarantinedMessage {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "transaction_id", nullable = false, length = 64)
    private String transactionId;

    @Column(name = "original_topic", nullable = false, length = 128)
    private String originalTopic;

    @Enumerated(EnumType.STRING)
    @Column(name = "error_category", nullable = false, length = 32)
    private ErrorCategory errorCategory;

    @Column(name = "exception_class", length = 256)
    private String exceptionClass;

    @Column(name = "exception_message", columnDefinition = "TEXT")
    private String exceptionMessage;

    @Column(name = "replay_attempts")
    private int replayAttempts;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private QuarantineStatus status;

    @Column(name = "quarantined_at", nullable = false)
    private Instant quarantinedAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    @Column(name = "resolved_by", length = 128)
    private String resolvedBy;

    @Column(name = "resolution_notes", columnDefinition = "TEXT")
    private String resolutionNotes;

    public void resolve(String by, String notes) {
        this.status = QuarantineStatus.RESOLVED;
        this.resolvedAt = Instant.now();
        this.resolvedBy = by;
        this.resolutionNotes = notes;
    }

    public void discard(String by, String notes) {
        this.status = QuarantineStatus.DISCARDED;
        this.resolvedAt = Instant.now();
        this.resolvedBy = by;
        this.resolutionNotes = notes;
    }
}
