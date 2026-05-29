package com.transactionshield.common.dlq;

/**
 * Kafka header name constants for the DLQ pipeline.
 *
 * Spring Kafka's DeadLetterPublishingRecoverer automatically adds the
 * {@code kafka_dlt-*} headers. The {@code x-*} headers are added by
 * our custom {@code setHeadersFunction} in each service's error handler.
 */
public final class DlqHeaders {

    // ── Spring built-in DLT headers (added by DeadLetterPublishingRecoverer) ──
    public static final String ORIGINAL_TOPIC     = "kafka_dlt-original-topic";
    public static final String ORIGINAL_OFFSET    = "kafka_dlt-original-offset";
    public static final String ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    public static final String EXCEPTION_FQCN     = "kafka_dlt-exception-fqcn";
    public static final String EXCEPTION_MESSAGE  = "kafka_dlt-exception-message";

    // ── Custom headers added by this system's enriched error handler ──
    /** {@link ErrorCategory} name (UTF-8 string) */
    public static final String ERROR_CATEGORY = "x-error-category";

    /** Number of times this message has been re-published to the original topic (string int) */
    public static final String REPLAY_COUNT   = "x-replay-count";

    private DlqHeaders() {}
}
