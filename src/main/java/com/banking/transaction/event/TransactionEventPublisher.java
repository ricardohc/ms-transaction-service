package com.banking.transaction.event;

import com.banking.transaction.document.TransactionDocument;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionEventPublisher {

    public static final String TOPIC = "banking.transaction.events.v1";
    private static final int EPOCH_SCALE = 9;

    private final ReactiveKafkaProducerTemplate<String, Object> kafkaTemplate;

    public Mono<Void> publishTransferCompleted(final TransactionDocument tx,
                                               final String eventType,
                                               final String correlationId) {
        final Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", UUID.randomUUID().toString());
        envelope.put("eventType", eventType);
        envelope.put("occurredAt", nowEpochSeconds());
        envelope.put("correlationId", correlationId);

        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", tx.getId().toString());
        payload.put("type", tx.getType());
        payload.put("status", tx.getStatus());
        // BigDecimal -> String sin notación científica
        payload.put("amount", tx.getAmount() != null
                ? tx.getAmount().toPlainString() : null);
        payload.put("currency", tx.getCurrency());
        payload.put("sourceAccountId", tx.getSourceId() != null
                ? tx.getSourceId().toString() : null);
        payload.put("targetAccountId", tx.getTargetId() != null
                ? tx.getTargetId().toString() : null);
        payload.put("description", tx.getDescription());
        payload.put("createdAt", tx.getCreatedAt() != null
                ? tx.getCreatedAt().toInstant(java.time.ZoneOffset.UTC).toString()
                : null);
        envelope.put("payload", payload);

        return kafkaTemplate.send(TOPIC, tx.getId().toString(), envelope)
                .doOnSuccess(v -> log.info("Transfer event {} published for tx {}",
                        eventType, tx.getId()))
                .doOnError(e -> log.error("Failed to publish transfer event {}",
                        eventType, e))
                .then();
    }

    public Mono<Void> publishStateEvent(final TransactionDocument tx,
                                        final String eventType,
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
        payload.put("amount", tx.getAmount() != null
                ? tx.getAmount().toPlainString() : null);
        payload.put("currency", tx.getCurrency());
        payload.put("sourceId", tx.getSourceId() != null
                ? tx.getSourceId().toString() : null);
        payload.put("targetId", tx.getTargetId() != null
                ? tx.getTargetId().toString() : null);
        payload.put("merchant", tx.getMerchant());
        envelope.put("payload", payload);

        return kafkaTemplate.send(TOPIC, tx.getId().toString(), envelope)
                .doOnSuccess(v -> log.info("Event {} published for tx {}",
                        eventType, tx.getId()))
                .doOnError(e -> log.error("Failed to publish event {}", eventType, e))
                .then();
    }

    /** epoch en segundos con 9 decimales, ej: 1794217851.270925100 */
    private BigDecimal nowEpochSeconds() {
        final Instant now = Instant.now();
        return BigDecimal.valueOf(now.getEpochSecond())
                .add(BigDecimal.valueOf(now.getNano(), EPOCH_SCALE))
                .setScale(EPOCH_SCALE, RoundingMode.HALF_UP);
    }
}