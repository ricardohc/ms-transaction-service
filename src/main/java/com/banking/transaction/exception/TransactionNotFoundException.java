package com.banking.transaction.exception;

/** Thrown when a transaction id does not exist. Maps to 404. */
public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(final String message) {
        super(message);
    }
}
