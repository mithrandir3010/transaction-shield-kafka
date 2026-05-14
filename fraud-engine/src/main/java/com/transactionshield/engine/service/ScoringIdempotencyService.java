package com.transactionshield.engine.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Prevents duplicate processing of the same Kafka message in the fraud engine.
 *
 * Kafka guarantees at-least-once delivery; the same transactionId can arrive
 * multiple times (e.g. consumer restart before offset commit). This guard
 * uses the same SET NX pattern as the producer's IdempotencyService but
 * keyed on transactionId (not idempotencyKey).
 *
 * Key prefix differs from the producer to avoid namespace collisions:
 *   Producer : idempotency:transaction:<idempotencyKey>
 *   Engine   : idempotency:scoring:<transactionId>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ScoringIdempotencyService {

    private static final String KEY_PREFIX = "idempotency:scoring:";

    private final StringRedisTemplate redisTemplate;

    @Value("${app.idempotency.scoring.ttl-hours:24}")
    private long ttlHours;

    /**
     * Tries to mark the transaction as being processed.
     *
     * @return true  → first time we see this transactionId, safe to process
     *         false → already processed (or in-flight), skip to avoid duplicate scoring
     */
    public boolean tryMarkAsProcessing(String transactionId) {
        String redisKey = KEY_PREFIX + transactionId;
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, "PROCESSING", Duration.ofHours(ttlHours));

        boolean isNew = Boolean.TRUE.equals(acquired);
        if (!isNew) {
            log.warn("Duplicate scoring attempt detected — transactionId={} skipping", transactionId);
        }
        return isNew;
    }

    /** Compensate: releases the lock when downstream publish fails. */
    public void release(String transactionId) {
        redisTemplate.delete(KEY_PREFIX + transactionId);
        log.warn("Scoring lock released (publish rollback) — transactionId={}", transactionId);
    }
}
