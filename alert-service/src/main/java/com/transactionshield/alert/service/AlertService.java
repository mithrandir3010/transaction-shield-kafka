package com.transactionshield.alert.service;

import com.transactionshield.alert.dto.AlertResponse;
import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.exception.AlertPersistenceException;
import com.transactionshield.alert.repository.AlertRepository;
import com.transactionshield.common.event.ScoredTransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Set;

/**
 * Core business logic for alert persistence and filtering.
 *
 * Retry strategy:
 *   @Retryable wraps @Transactional (outer → inner via AOP order).
 *   On each retry, the previous transaction is rolled back and a new
 *   one begins — correct behaviour for transient DB failures (connection
 *   pool exhaustion, deadlock, network hiccup to PostgreSQL).
 *
 *   DataIntegrityViolationException (unique constraint on transaction_id)
 *   is explicitly excluded: it signals a duplicate and must not be retried.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AlertService {

    private final AlertRepository alertRepository;

    @Value("#{'${app.alert.notify-risk-levels:HIGH,CRITICAL}'.split(',')}")
    private Set<String> notifyRiskLevels;

    // ── Persistence ──────────────────────────────────────────────────

    @Retryable(
            retryFor    = DataAccessException.class,
            noRetryFor  = DataIntegrityViolationException.class,   // duplicate → skip immediately
            maxAttempts = 3,
            backoff     = @Backoff(delay = 500, multiplier = 2.0, maxDelay = 4_000)
    )
    @Transactional
    public Alert saveAlert(ScoredTransactionEvent event) {
        // DB-level duplicate guard (belt-and-suspenders alongside the UNIQUE constraint)
        return alertRepository.findByTransactionId(event.transactionId())
                .map(existing -> {
                    log.warn("Alert already exists — transactionId={} alertId={} skipping",
                            event.transactionId(), existing.getId());
                    return existing;
                })
                .orElseGet(() -> {
                    Alert alert = Alert.builder()
                            .transactionId(event.transactionId())
                            .userId(event.userId())
                            .fraudScore(event.fraudScore())
                            .riskLevel(event.riskLevel().name())
                            .triggeredRules(String.join(",", event.triggeredRules()))
                            .status("OPEN")
                            .createdAt(Instant.now())
                            .updatedAt(Instant.now())
                            .build();

                    Alert saved = alertRepository.save(alert);
                    log.info("Alert persisted — id={} transactionId={} fraudScore={} riskLevel={}",
                            saved.getId(), saved.getTransactionId(),
                            saved.getFraudScore(), saved.getRiskLevel());
                    return saved;
                });
    }

    /**
     * Called by Spring Retry after all @Retryable attempts fail.
     * Wraps the root cause and rethrows so the Kafka consumer propagates
     * the error to the DefaultErrorHandler (which routes to DLQ).
     */
    @Recover
    public Alert recoverFromDbFailure(DataAccessException ex, ScoredTransactionEvent event) {
        log.error("All retry attempts exhausted — transactionId={} cause={}",
                event.transactionId(), ex.getMessage());
        throw new AlertPersistenceException(
                "Persistent DB failure for transactionId=" + event.transactionId(), ex);
    }

    // ── Query ─────────────────────────────────────────────────────────

    @Transactional(readOnly = true)
    public Page<AlertResponse> listAlerts(Pageable pageable, String riskLevel) {
        return alertRepository
                .findAlertsFiltered(riskLevel, pageable)
                .map(AlertResponse::from);
    }

    // ── Notification decision ─────────────────────────────────────────

    public boolean shouldNotify(ScoredTransactionEvent event) {
        return notifyRiskLevels.contains(event.riskLevel().name());
    }
}
