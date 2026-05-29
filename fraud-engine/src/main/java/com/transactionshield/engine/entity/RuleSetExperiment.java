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
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * Defines an active A/B experiment for the rule engine.
 *
 * When active, {@code trafficPct}% of incoming transactions are evaluated
 * with the EXPERIMENT rule variant instead of STABLE. Routing is deterministic:
 *   abs(hash(transactionId)) % 100 < trafficPct → EXPERIMENT
 *
 * Only one experiment should be active at a time (enforced by the service layer).
 */
@Entity
@Table(name = "rule_set_experiments")
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RuleSetExperiment {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, length = 128)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "traffic_pct", nullable = false)
    private int trafficPct;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "created_by", length = 128)
    private String createdBy;

    @Column(name = "activated_at")
    private Instant activatedAt;

    @Column(name = "deactivated_at")
    private Instant deactivatedAt;

    public void activate() {
        this.active = true;
        this.activatedAt = Instant.now();
        this.deactivatedAt = null;
    }

    public void deactivate() {
        this.active = false;
        this.deactivatedAt = Instant.now();
    }
}
