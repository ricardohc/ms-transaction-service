package com.banking.transaction.mapper;

import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.OperationAcceptedResponse;
import com.banking.transaction.dto.TransactionResponse;
import com.banking.transaction.dto.TransactionStatus;
import com.banking.transaction.dto.TransactionType;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.UUID;

public final class TransactionMapper {

    private TransactionMapper() {
    }

    public static TransactionResponse toResponse(final TransactionDocument doc) {
        final TransactionResponse response = new TransactionResponse();
        response.setId(doc.getId());
        if (doc.getType() != null) {
            response.setType(TransactionType.valueOf(doc.getType()));
        }
        if (doc.getStatus() != null) {
            response.setStatus(TransactionStatus.valueOf(doc.getStatus()));
        }
        if (doc.getAmount() != null) {
            // BigDecimal -> String sin notación científica
            response.amount(doc.getAmount().toPlainString());
        }
        response.setCurrency(doc.getCurrency());
        response.setSourceId(doc.getSourceId());
        response.setTargetId(NullableMapper.of(doc.getTargetId()));
        response.setSourceWalletPhoneNumber(doc.getSourceWalletPhoneNumber());
        response.setTargetPhoneNumber(doc.getTargetPhoneNumber());
        if (doc.getCreatedAt() != null) {
            response.setCreatedAt(doc.getCreatedAt().atOffset(ZoneOffset.UTC));
        }
        if (doc.getCompletedAt() != null) {
            response.setCompletedAt(doc.getCompletedAt().atOffset(ZoneOffset.UTC));
        }
        response.setFailureReason(doc.getFailureReason());
        return response;
    }

    public static OperationAcceptedResponse toAccepted(final TransactionDocument doc) {
        final OperationAcceptedResponse response = new OperationAcceptedResponse();
        response.setOperationId(UUID.randomUUID());
        response.setTransactionId(doc.getTransactionId() != null
                ? doc.getTransactionId()
                : doc.getId());
        response.setStatus(OperationAcceptedResponse.StatusEnum.ACCEPTED);
        return response;
    }

    /**
     * Utilidad: normaliza un BigDecimal a 2 decimales HALF_UP.
     * Útil para montos que vienen de fuentes externas.
     */
    public static BigDecimal normalizeMoney(final BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.setScale(2, java.math.RoundingMode.HALF_UP);
    }
}