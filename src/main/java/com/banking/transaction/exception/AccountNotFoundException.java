package com.banking.transaction.exception;

import lombok.Getter;

@Getter
public class AccountNotFoundException extends RuntimeException {

    private final String accountId;

    public AccountNotFoundException(final String accountId) {
        super("La cuenta " + accountId + " no existe en el sistema");
        this.accountId = accountId;
    }

}