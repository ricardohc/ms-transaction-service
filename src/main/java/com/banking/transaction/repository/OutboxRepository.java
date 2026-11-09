package com.banking.transaction.repository;

import java.util.UUID;

import com.banking.transaction.document.OutboxDocument;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface OutboxRepository extends ReactiveCrudRepository<OutboxDocument, UUID> {

    Flux<OutboxDocument> findByStatusOrderByOccurredAtAsc(String status);
}
