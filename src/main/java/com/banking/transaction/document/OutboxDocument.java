package com.banking.transaction.document;

import java.time.LocalDateTime;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "transaction_outbox")
public class OutboxDocument {

    @Id
    private UUID id;

    @Indexed(unique = true)
    private String eventId;

    private UUID transactionId;

    private String eventType;

    private String topic;

    private String aggregateId;

    private String aggregateType;

    private String producer;

    private LocalDateTime occurredAt;

    private String correlationId;

    private String payloadJson;

    /** PENDING, PUBLISHED, FAILED. */
    private String status;

    private int attempts;
}
