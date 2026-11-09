package com.banking.transaction.service;

import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.TransactionCreateRequest;
import io.reactivex.rxjava3.core.Single;
import java.util.UUID;

/**
 * Transaction orchestration service. Pipelines are expressed with RxJava 3
 * (Single/Maybe/Flowable) as required by the skill contract.
 */
public interface TransactionService {

    /**
     * Creates the transaction idempotently, persists it with an outbox entry
     * and schedules asynchronous orchestration.
     *
     * @param transactionId X-Transaction-ID header for idempotency
     * @param correlationId X-Correlation-ID header for tracing
     * @param request       polymorphic request body
     * @return the persisted transaction document
     */
    Single<TransactionDocument> createTransaction(UUID transactionId, String correlationId,
            TransactionCreateRequest request);

    /** Finds a transaction by id or errors with TransactionNotFoundException. */
    Single<TransactionDocument> findById(UUID transactionId);
}
