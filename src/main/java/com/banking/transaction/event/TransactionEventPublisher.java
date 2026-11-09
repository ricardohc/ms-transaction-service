package com.banking.transaction.event;

import com.banking.transaction.document.TransactionDocument;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Publishes transaction state events to banking.transaction.events.v1 using
 * the mandatory envelope. Never includes sensitive data (JWT, PAN, OTP).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventPublisher {

    /** Topic defined by the skill contract. */
    public static final String TOPIC = "banking.transaction.events.v1";

    private final ReactiveKafkaProducerTemplate<String, Object> kafkaTemplate;

    /** Publishes TransactionAccepted for the given transaction. */
    public void publishAccepted(final TransactionDocument tx, final String correlationId) {
        publish("TransactionAccepted", tx, correlationId);
    }

    private void publish(final String eventType, final TransactionDocument tx,
            final String correlationId) {
        final Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", UUID.randomUUID().toString());
        envelope.put("eventType", eventType);
        envelope.put("eventVersion", 1);
        envelope.put("aggregateId", tx.getId().toString());
        envelope.put("aggregateType", "Transaction");
        envelope.put("occurredAt", OffsetDateTime.now().toString());
        envelope.put("correlationId", correlationId);
        envelope.put("transactionId", tx.getId().toString());
        envelope.put("producer", "ms-transaction-service");
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", tx.getType());
        payload.put("status", tx.getStatus());
        envelope.put("payload", payload);
        kafkaTemplate.send(TOPIC, tx.getId().toString(), envelope)
                .doOnSuccess(v -> log.debug("{} published for transactionId={}",
                        eventType, tx.getId()))
                .doOnError(e -> log.error("Failed to publish {} for transactionId={}",
                        eventType, tx.getId(), e))
                .onErrorResume(e -> Mono.empty())
                .subscribe();
    }
}
