package com.transactionshield.engine.dlq;

import com.transactionshield.common.dlq.ErrorCategory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Maps a throwable to an {@link ErrorCategory} for DLQ header enrichment.
 *
 * Rules (evaluated top-down, cause chain is traversed once):
 *   DeserializationException          → FATAL        (broken payload, schema issue)
 *   DataIntegrityViolationException   → NON_RETRYABLE (duplicate / business constraint)
 *   IllegalArgumentException          → NON_RETRYABLE (invalid input, replay won't help)
 *   Everything else (incl. wrapped)   → TRANSIENT    (infra failure, safe to replay)
 */
public final class ErrorClassifier {

    public static ErrorCategory classify(Throwable ex) {
        if (ex == null) return ErrorCategory.TRANSIENT;

        if (ex instanceof DeserializationException) return ErrorCategory.FATAL;

        if (ex instanceof DataIntegrityViolationException
                || ex instanceof IllegalArgumentException) {
            return ErrorCategory.NON_RETRYABLE;
        }

        Throwable cause = ex.getCause();
        if (cause != null && cause != ex) {
            return classify(cause);
        }

        return ErrorCategory.TRANSIENT;
    }

    private ErrorClassifier() {}
}
