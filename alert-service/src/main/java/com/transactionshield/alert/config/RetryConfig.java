package com.transactionshield.alert.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

/**
 * Activates Spring Retry's AOP proxy, which intercepts @Retryable methods.
 *
 * Retry proxy order matters:
 *   @Retryable (outer) wraps @Transactional (inner)
 *   → each retry attempt starts a fresh transaction
 *   → DB failure in attempt N rolls back cleanly before attempt N+1
 */
@Configuration
@EnableRetry
public class RetryConfig {}
