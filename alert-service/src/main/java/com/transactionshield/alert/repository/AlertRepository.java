package com.transactionshield.alert.repository;

import com.transactionshield.alert.entity.Alert;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface AlertRepository extends JpaRepository<Alert, UUID> {

    /**
     * DB-level duplicate guard — checked before attempting insert.
     * Prevents double-persist when at-least-once Kafka redelivers a message.
     */
    Optional<Alert> findByTransactionId(String transactionId);

    /**
     * Unified list query with optional riskLevel filter.
     * Passing null for riskLevel returns all rows.
     */
    @Query("""
            SELECT a FROM Alert a
            WHERE (:riskLevel IS NULL OR UPPER(a.riskLevel) = UPPER(:riskLevel))
            ORDER BY a.createdAt DESC
            """)
    Page<Alert> findAlertsFiltered(@Param("riskLevel") String riskLevel, Pageable pageable);

    /** Counts open alerts by risk level — useful for dashboard metrics. */
    long countByRiskLevelIgnoreCaseAndStatus(String riskLevel, String status);
}
