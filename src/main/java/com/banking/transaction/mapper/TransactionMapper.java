package com.banking.transaction.mapper;

import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.OperationAcceptedResponse;
import com.banking.transaction.dto.TransactionResponse;
import com.banking.transaction.dto.TransactionStatus;
import com.banking.transaction.dto.TransactionType;
import java.util.UUID;

/** Maps between transaction documents and contract DTOs. */
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
            response.amount(doc.getAmount().toPlainString());
        }
        response.setCurrency(doc.getCurrency());
        response.setSourceId(doc.getSourceId());
        response.setTargetId(com.banking.transaction.mapper.NullableMapper.of(doc.getTargetId()));
        response.setSourceWalletPhoneNumber(doc.getSourceWalletPhoneNumber());
        response.setTargetPhoneNumber(doc.getTargetPhoneNumber());
        return response;
    }

    public static OperationAcceptedResponse toAccepted(final TransactionDocument doc) {
        final OperationAcceptedResponse response = new OperationAcceptedResponse();
        response.setOperationId(UUID.randomUUID());
        response.setTransactionId(doc.getId());
        response.setStatus(OperationAcceptedResponse.StatusEnum.ACCEPTED);
        return response;
    }
}
