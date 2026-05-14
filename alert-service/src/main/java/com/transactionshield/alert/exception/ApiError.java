package com.transactionshield.alert.exception;

import java.time.Instant;

public record ApiError(int status, String error, String message, String path, Instant timestamp) {

    public static ApiError of(int status, String error, String message, String path) {
        return new ApiError(status, error, message, path, Instant.now());
    }
}
