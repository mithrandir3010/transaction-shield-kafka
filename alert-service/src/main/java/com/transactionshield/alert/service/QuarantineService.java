package com.transactionshield.alert.service;

import com.transactionshield.alert.entity.QuarantinedMessage;
import com.transactionshield.alert.entity.QuarantineStatus;
import com.transactionshield.alert.repository.QuarantinedMessageRepository;
import com.transactionshield.common.dlq.ErrorCategory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class QuarantineService {

    private final QuarantinedMessageRepository repository;
    private final MeterRegistry meterRegistry;

    /**
     * Persists a quarantined message idempotently.
     * A second call with the same (transactionId, originalTopic) returns the existing record.
     */
    @Transactional
    public QuarantinedMessage quarantine(
            String transactionId,
            String originalTopic,
            ErrorCategory errorCategory,
            String exceptionClass,
            String exceptionMessage,
            int replayAttempts) {

        return repository.findByTransactionIdAndOriginalTopic(transactionId, originalTopic)
                .orElseGet(() -> {
                    QuarantinedMessage msg = QuarantinedMessage.builder()
                            .transactionId(transactionId)
                            .originalTopic(originalTopic != null ? originalTopic : "unknown")
                            .errorCategory(errorCategory)
                            .exceptionClass(exceptionClass)
                            .exceptionMessage(exceptionMessage)
                            .replayAttempts(replayAttempts)
                            .status(QuarantineStatus.OPEN)
                            .quarantinedAt(Instant.now())
                            .build();

                    QuarantinedMessage saved = repository.save(msg);

                    meterRegistry.counter("quarantine.message.total",
                            "error_category", errorCategory.name()).increment();

                    log.warn("[QUARANTINE] Message quarantined — transactionId={} topic={} category={} attempts={}",
                            transactionId, originalTopic, errorCategory, replayAttempts);

                    return saved;
                });
    }

    @Transactional(readOnly = true)
    public Page<QuarantinedMessage> list(QuarantineStatus status, Pageable pageable) {
        return status != null
                ? repository.findByStatus(status, pageable)
                : repository.findAll(pageable);
    }

    @Transactional
    public QuarantinedMessage resolve(UUID id, String resolvedBy, String notes) {
        QuarantinedMessage msg = findOrThrow(id);
        msg.resolve(resolvedBy, notes);
        log.info("[QUARANTINE] Resolved — id={} by={}", id, resolvedBy);
        return repository.save(msg);
    }

    @Transactional
    public QuarantinedMessage discard(UUID id, String resolvedBy, String notes) {
        QuarantinedMessage msg = findOrThrow(id);
        msg.discard(resolvedBy, notes);
        log.info("[QUARANTINE] Discarded — id={} by={}", id, resolvedBy);
        return repository.save(msg);
    }

    private QuarantinedMessage findOrThrow(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Quarantined message not found: " + id));
    }
}
