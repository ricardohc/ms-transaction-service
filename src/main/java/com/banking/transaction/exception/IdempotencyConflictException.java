package com.banking.transaction.exception;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(final String message) {
        super(message);
    }
}