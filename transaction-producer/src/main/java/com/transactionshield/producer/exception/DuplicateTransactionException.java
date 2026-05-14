package com.transactionshield.producer.exception;

public class DuplicateTransactionException extends RuntimeException {

    private final String idempotencyKey;

    public DuplicateTransactionException(String idempotencyKey) {
        super("Duplicate transaction detected for idempotency key: " + idempotencyKey);
        this.idempotencyKey = idempotencyKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}
