package com.banking.transaction.exception;

public class InvalidTransactionException extends RuntimeException {

    public InvalidTransactionException(final String message) {
        super(message);
    }
}
