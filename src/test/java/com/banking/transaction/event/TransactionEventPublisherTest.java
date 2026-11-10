package com.banking.transaction.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.banking.transaction.document.TransactionDocument;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

class TransactionEventPublisherTest {

    private ReactiveKafkaProducerTemplate<String, Object> kafkaTemplate;
    private TransactionEventPublisher publisher;

    private final UUID txId = UUID.randomUUID();
    private final UUID sourceId = UUID.randomUUID();
    private final UUID targetId = UUID.randomUUID();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafkaTemplate = Mockito.mock(ReactiveKafkaProducerTemplate.class);
        SenderResult result = Mockito.mock(SenderResult.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(Object.class)))
                .thenReturn(Mono.just(result));
        publisher = new TransactionEventPublisher(kafkaTemplate);
    }

    private TransactionDocument tx(String status) {
        return TransactionDocument.builder()
                .id(txId)
                .transactionId(txId)
                .type("ACCOUNT_TRANSFER")
                .status(status)
                .amount(new BigDecimal("150.00"))
                .currency("PEN")
                .sourceId(sourceId)
                .targetId(targetId)
                .description("test")
                .createdAt(LocalDateTime.of(2024, 6, 1, 12, 0))
                .build();
    }

    @Test
    void publishTransferCompletedSendsCorrectEnvelope() {
        StepVerifier.create(publisher.publishTransferCompleted(
                        tx("COMPLETED"), "TransactionCompleted", "corr-1"))
                .verifyComplete();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(1))
                .send(eq(TransactionEventPublisher.TOPIC), eq(txId.toString()), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, Object> envelope = (Map<String, Object>) captor.getValue();
        org.junit.jupiter.api.Assertions.assertEquals("TransactionCompleted",
                envelope.get("eventType"));
        org.junit.jupiter.api.Assertions.assertEquals("corr-1", envelope.get("correlationId"));
        org.junit.jupiter.api.Assertions.assertNotNull(envelope.get("eventId"));
        org.junit.jupiter.api.Assertions.assertNotNull(envelope.get("occurredAt"));

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
        org.junit.jupiter.api.Assertions.assertEquals(txId.toString(), payload.get("id"));
        org.junit.jupiter.api.Assertions.assertEquals("ACCOUNT_TRANSFER", payload.get("type"));
        org.junit.jupiter.api.Assertions.assertEquals("COMPLETED", payload.get("status"));
        org.junit.jupiter.api.Assertions.assertEquals("150.00", payload.get("amount"));
        org.junit.jupiter.api.Assertions.assertEquals("PEN", payload.get("currency"));
        org.junit.jupiter.api.Assertions.assertEquals(sourceId.toString(),
                payload.get("sourceAccountId"));
        org.junit.jupiter.api.Assertions.assertEquals(targetId.toString(),
                payload.get("targetAccountId"));
    }

    @Test
    void publishTransferCompletedHandlesNullOptionalFields() {
        TransactionDocument tx = TransactionDocument.builder()
                .id(txId)
                .type("CARD_CONSUMPTION")
                .status("COMPLETED")
                .amount(new BigDecimal("50.00"))
                .currency("PEN")
                .sourceId(null)
                .targetId(null)
                .createdAt(null)
                .build();

        StepVerifier.create(publisher.publishTransferCompleted(
                        tx, "TransactionCompleted", "corr"))
                .verifyComplete();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(anyString(), anyString(), captor.capture());
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>)
                ((Map<String, Object>) captor.getValue()).get("payload");
        org.junit.jupiter.api.Assertions.assertNull(payload.get("sourceAccountId"));
        org.junit.jupiter.api.Assertions.assertNull(payload.get("targetAccountId"));
        org.junit.jupiter.api.Assertions.assertNull(payload.get("createdAt"));
    }

    @Test
    void publishStateEventSendsCorrectEnvelope() {
        StepVerifier.create(publisher.publishStateEvent(
                        tx("PENDING"), "TransactionAccepted", "corr-2"))
                .verifyComplete();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TransactionEventPublisher.TOPIC),
                eq(txId.toString()), captor.capture());

        @SuppressWarnings("unchecked")
        Map<String, Object> envelope = (Map<String, Object>) captor.getValue();
        org.junit.jupiter.api.Assertions.assertEquals("TransactionAccepted",
                envelope.get("eventType"));
        org.junit.jupiter.api.Assertions.assertEquals(1, envelope.get("eventVersion"));
        org.junit.jupiter.api.Assertions.assertEquals(txId.toString(),
                envelope.get("aggregateId"));
        org.junit.jupiter.api.Assertions.assertEquals("Transaction",
                envelope.get("aggregateType"));
        org.junit.jupiter.api.Assertions.assertEquals("ms-transaction-service",
                envelope.get("producer"));
        org.junit.jupiter.api.Assertions.assertEquals("corr-2",
                envelope.get("correlationId"));
        org.junit.jupiter.api.Assertions.assertEquals(txId.toString(),
                envelope.get("transactionId"));

        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
        org.junit.jupiter.api.Assertions.assertEquals("PENDING", payload.get("status"));
        org.junit.jupiter.api.Assertions.assertEquals("150.00", payload.get("amount"));
        org.junit.jupiter.api.Assertions.assertEquals("PEN", payload.get("currency"));
    }

    @Test
    void publishTransferCompletedPropagatesKafkaError() {
        when(kafkaTemplate.send(anyString(), anyString(), any(Object.class)))
                .thenReturn(Mono.error(new RuntimeException("kafka down")));

        StepVerifier.create(publisher.publishTransferCompleted(
                        tx("COMPLETED"), "TransactionCompleted", "corr"))
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void eachEventGetsUniqueEventId() {
        StepVerifier.create(publisher.publishStateEvent(
                tx("PENDING"), "TransactionAccepted", "c")).verifyComplete();
        StepVerifier.create(publisher.publishStateEvent(
                tx("PENDING"), "TransactionAccepted", "c")).verifyComplete();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), captor.capture());
        var envelopes = captor.getAllValues();
        @SuppressWarnings("unchecked")
        var id1 = ((Map<String, Object>) envelopes.get(0)).get("eventId");
        @SuppressWarnings("unchecked")
        var id2 = ((Map<String, Object>) envelopes.get(1)).get("eventId");
        org.junit.jupiter.api.Assertions.assertNotEquals(id1, id2);
    }
}