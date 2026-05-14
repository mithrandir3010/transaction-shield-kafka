package com.transactionshield.producer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Redis-backed idempotency guard using SET NX (set-if-not-exists).
 *
 * Flow:
 *   1. tryAcquire()  → SET idempotency:<key> "PROCESSING" NX EX <ttl>
 *      - returns true  → lock acquired, proceed
 *      - returns false → duplicate detected, return 409
 *   2. On Kafka failure → release() deletes the key so the client can retry.
 *
 * TTL ensures keys are auto-evicted, preventing unbounded growth.
 * Redis is the primary store; PostgreSQL idempotency_log is the durable fallback
 * for post-mortem auditing (written by the fraud-engine after successful scoring).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:transaction:";

    private final StringRedisTemplate redisTemplate;

    @Value("${app.idempotency.ttl-hours:24}")
    private long ttlHours;

    /**
     * Attempts to acquire the idempotency lock for the given key.
     *
     * @return true if the lock was acquired (new request),
     *         false if the key already exists (duplicate)
     */
    public boolean tryAcquire(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        // setIfAbsent maps directly to Redis SET NX EX — atomic operation
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, "PROCESSING", Duration.ofHours(ttlHours));

        boolean result = Boolean.TRUE.equals(acquired);
        log.debug("Idempotency lock {} — redisKey={}", result ? "ACQUIRED" : "ALREADY_EXISTS", redisKey);
        return result;
    }

    /**
     * Releases the lock by deleting the Redis key.
     * Called as a compensating action when Kafka publish fails,
     * allowing the client to safely retry the same request.
     */
    public void release(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        Boolean deleted = redisTemplate.delete(redisKey);
        log.warn("Idempotency lock RELEASED (Kafka rollback) — redisKey={}, deleted={}", redisKey, deleted);
    }
}
