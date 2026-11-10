package com.banking.transaction.document;

import java.math.BigDecimal;
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
@Document(collection = "transactions")
public class TransactionDocument {

    @Id
    private UUID id;

    @Indexed(unique = true)
    private UUID transactionId;

    private String type;

    private String status;

    private BigDecimal amount;

    private String currency;

    private UUID sourceId;

    private UUID targetId;

    private String sourceWalletPhoneNumber;

    private String targetPhoneNumber;

    private UUID creditId;

    private UUID cardId;

    private String merchant;

    private String description;

    private LocalDateTime createdAt;

    private LocalDateTime completedAt;

    private String failureReason;
}
