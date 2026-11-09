package com.banking.transaction.exception;

/** Thrown when the request payload is invalid for its declared type. Maps to 422. */
public class InvalidTransactionException extends RuntimeException {

    public InvalidTransactionException(final String message) {
        super(message);
    }
}
