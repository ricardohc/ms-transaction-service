package com.banking.transaction.exception;

public class BusinessValidationException extends RuntimeException {

    public BusinessValidationException(final String message) {
        super(message);
    }
}