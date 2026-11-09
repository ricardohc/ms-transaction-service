package com.banking.transaction.controller;

import com.banking.transaction.api.TransactionApi;
import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.OperationAcceptedResponse;
import com.banking.transaction.dto.TransactionCreateRequest;
import com.banking.transaction.dto.TransactionResponse;
import com.banking.transaction.mapper.TransactionMapper;
import com.banking.transaction.service.TransactionService;
import com.banking.transaction.service.impl.RxJavaBridge;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/** REST adapter implementing the generated TransactionApi contract. */
@RestController
@RequiredArgsConstructor
public class TransactionApiImpl implements TransactionApi {

    private final TransactionService transactionService;
    private final RxJavaBridge bridge;

    @Override
    public Mono<ResponseEntity<OperationAcceptedResponse>> createTransaction(
            final String xCorrelationID, final String xChannelID, final UUID xTransactionID,
            final Mono<TransactionCreateRequest> transactionCreateRequest,
            final ServerWebExchange exchange) {
        return transactionCreateRequest
                .flatMap(req -> bridge.toMono(
                        transactionService.createTransaction(xTransactionID, xCorrelationID, req)))
                .map(tx -> ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(TransactionMapper.toAccepted(tx)));
    }

    @Override
    public Mono<ResponseEntity<TransactionResponse>> findTransactionById(
            final UUID transactionId, final String xCorrelationID, final String xChannelID,
            final ServerWebExchange exchange) {
        return bridge.toMono(transactionService.findById(transactionId))
                .map(tx -> ResponseEntity.ok(TransactionMapper.toResponse(tx)));
    }
}
