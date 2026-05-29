package com.transactionshield.alert.entity;

public enum QuarantineStatus {
    /** Awaiting operator review — default state on arrival */
    OPEN,
    /** Operator confirmed the root cause is fixed; message may be replayed manually */
    RESOLVED,
    /** Operator decided to discard (test data, irrelevant event, schema-incompatible) */
    DISCARDED
}
