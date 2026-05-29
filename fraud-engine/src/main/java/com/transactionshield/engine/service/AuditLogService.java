package com.transactionshield.engine.service;

import com.transactionshield.engine.entity.ScoringAuditLog;
import com.transactionshield.engine.repository.ScoringAuditLogRepository;
import com.transactionshield.engine.scoring.ScoringResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Writes an immutable audit record after every scoring.
 *
 * Written asynchronously so DB latency does not add to the Kafka consumer
 * processing time. The record is never updated or deleted — it serves as
 * a permanent ledger for compliance and A/B analysis.
 *
 * If the async write fails, the error is logged but processing continues —
 * audit log loss is preferable to blocking production scoring.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AuditLogService {

    private final ScoringAuditLogRepository auditLogRepository;

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(ScoringResult result) {
        try {
            ScoringAuditLog entry = ScoringAuditLog.builder()
                    .transactionId(result.transactionId())
                    .variant(result.variant())
                    .rawScore(result.rawScore())
                    .fraudScore(result.fraudScore())
                    .riskLevel(result.riskLevel().name())
                    .triggeredRules(String.join(",", result.triggeredRuleCodes()))
                    .evaluatedAt(Instant.now())
                    .build();

            auditLogRepository.save(entry);
        } catch (Exception e) {
            log.error("[AUDIT] Failed to persist audit log for transactionId={}: {}",
                    result.transactionId(), e.getMessage(), e);
        }
    }

    @Transactional(readOnly = true)
    public List<ScoringAuditLog> findByTransactionId(String transactionId) {
        return auditLogRepository.findByTransactionId(transactionId);
    }

    @Transactional(readOnly = true)
    public Page<ScoringAuditLog> findByVariant(String variant, Pageable pageable) {
        return auditLogRepository.findByVariant(variant, pageable);
    }
}
