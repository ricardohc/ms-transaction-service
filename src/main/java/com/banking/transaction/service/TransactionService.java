package com.banking.transaction.service;

import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.TransactionCreateRequest;
import io.reactivex.rxjava3.core.Single;
import java.util.UUID;

public interface TransactionService {

    Single<TransactionDocument> createTransaction(UUID transactionId, String correlationId,
            TransactionCreateRequest request);

    Single<TransactionDocument> findById(UUID transactionId);
}
