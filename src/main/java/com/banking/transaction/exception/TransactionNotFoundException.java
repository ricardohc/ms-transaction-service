package com.banking.transaction.exception;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(final String message) {
        super(message);
    }
}
