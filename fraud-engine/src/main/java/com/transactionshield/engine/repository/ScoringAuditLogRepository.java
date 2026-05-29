package com.transactionshield.engine.repository;

import com.transactionshield.engine.entity.ScoringAuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ScoringAuditLogRepository extends JpaRepository<ScoringAuditLog, UUID> {

    List<ScoringAuditLog> findByTransactionId(String transactionId);

    Page<ScoringAuditLog> findByVariant(String variant, Pageable pageable);
}
