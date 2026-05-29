package com.transactionshield.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(
        name = "fraud_rules",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_fraud_rules_code_variant",
                columnNames = {"rule_code", "variant"}
        )
)
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FraudRuleEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "rule_code", nullable = false, length = 64)
    private String ruleCode;

    @Column(length = 500)
    private String description;

    @Column(name = "score_weight", nullable = false)
    private int scoreWeight;

    @Column(nullable = false)
    private boolean enabled;

    @Column(nullable = false, length = 32)
    private String variant;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, String> parameters;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
