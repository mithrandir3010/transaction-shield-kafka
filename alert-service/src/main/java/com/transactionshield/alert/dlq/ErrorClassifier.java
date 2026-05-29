package com.transactionshield.alert.dlq;

import com.transactionshield.common.dlq.ErrorCategory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Maps a throwable to an {@link ErrorCategory} for DLQ header enrichment.
 *
 * Alert-service specific notes:
 *   AlertPersistenceException wraps DataAccessException → TRANSIENT (infra failure)
 *   DataIntegrityViolationException (duplicate alert)   → NON_RETRYABLE
 *   DeserializationException (Avro schema mismatch)    → FATAL
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
