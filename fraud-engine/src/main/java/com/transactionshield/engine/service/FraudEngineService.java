package com.transactionshield.engine.service;

import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.producer.ScoredTransactionProducer;
import com.transactionshield.engine.scoring.FraudRuleEngine;
import com.transactionshield.engine.scoring.ScoringResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Orchestrates the full fraud evaluation lifecycle for a single transaction:
 *
 *  [1] Idempotency guard  — skip if already processed (Kafka at-least-once)
 *  [2] Rule evaluation    — FraudRuleEngine applies all registered FraudRules
 *  [3] Event assembly     — builds ScoredTransactionEvent from original + results
 *  [4] Publish            — sends to transactions.scored
 *  [5] Compensate         — releases idempotency lock on publish failure
 *                           so the consumer can retry and the DLQ handles final failures
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class FraudEngineService {

    private final ScoringIdempotencyService idempotencyService;
    private final FraudRuleEngine           ruleEngine;
    private final ScoredTransactionProducer scoredProducer;

    public void process(TransactionEvent event) throws Exception {

        // ── [1] Idempotency ──────────────────────────────────────────
        if (!idempotencyService.tryMarkAsProcessing(event.transactionId())) {
            log.info("Skipping already-processed transactionId={}", event.transactionId());
            return;
        }

        // ── [2] Rule evaluation ──────────────────────────────────────
        ScoringResult result = ruleEngine.evaluate(event);

        // ── [3] Assemble output event ────────────────────────────────
        ScoredTransactionEvent scored = new ScoredTransactionEvent(
                event.transactionId(),
                event.idempotencyKey(),
                event.userId(),
                event.amount(),
                event.currency(),
                event.country(),
                event.deviceFingerprint(),
                event.timestamp(),
                result.rawScore(),
                result.fraudScore(),
                result.riskLevel(),
                result.triggeredRuleCodes(),
                Instant.now()
        );

        // ── [4] Publish to transactions.scored ───────────────────────
        try {
            scoredProducer.publish(scored);
        } catch (Exception ex) {
            // ── [5] Compensate ────────────────────────────────────────
            // Release the idempotency lock so the consumer can retry this
            // message (or the DLQ error handler can take over after max retries)
            idempotencyService.release(event.transactionId());
            throw ex; // rethrow so DefaultErrorHandler triggers retry/DLQ
        }
    }
}
