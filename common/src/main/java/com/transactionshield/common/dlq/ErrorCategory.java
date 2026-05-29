package com.transactionshield.common.dlq;

/**
 * Classifies a DLQ failure so downstream operators and the replay service
 * can decide the right action without parsing exception stack traces.
 *
 * Decision table:
 *   TRANSIENT    → retry after infra fix (DB reconnect, network recovery)
 *   NON_RETRYABLE → discard after review (duplicate, business rule violation)
 *   FATAL        → quarantine immediately (schema mismatch, corrupt payload)
 */
public enum ErrorCategory {

    /**
     * Transient infrastructure failure — DB connection refused, network timeout.
     * Safe to replay once the underlying issue is resolved.
     */
    TRANSIENT,

    /**
     * Business-rule rejection — duplicate event, unique constraint violation.
     * Replaying produces the same result; operator must review and discard.
     */
    NON_RETRYABLE,

    /**
     * Unrecoverable message — deserialization failure, schema incompatibility.
     * The payload itself is broken; replay requires a schema fix and redeploy.
     */
    FATAL
}
