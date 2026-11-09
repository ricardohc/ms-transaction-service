package com.banking.transaction.repository;

import java.util.UUID;

import com.banking.transaction.document.TransactionDocument;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface TransactionRepository extends ReactiveCrudRepository<TransactionDocument, UUID> {

    Mono<TransactionDocument> findByTransactionId(UUID transactionId);
}
