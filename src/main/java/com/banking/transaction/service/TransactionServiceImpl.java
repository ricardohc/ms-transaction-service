package com.banking.transaction.service;

import com.banking.transaction.client.SagaOrchestratorClient;
import com.banking.transaction.client.SagaOrchestratorClient.AccountDto;
import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.dto.CardConsumptionTransaction;
import com.banking.transaction.dto.CreditPaymentTransaction;
import com.banking.transaction.dto.TransactionCreateRequest;
import com.banking.transaction.dto.WalletTransferTransaction;
import com.banking.transaction.event.TransactionEventPublisher;
import com.banking.transaction.exception.IdempotencyConflictException;
import com.banking.transaction.exception.InvalidTransactionException;
import com.banking.transaction.exception.TransactionNotFoundException;
import com.banking.transaction.repository.TransactionRepository;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionServiceImpl implements TransactionService {

    private final TransactionRepository transactionRepository;
    private final TransactionEventPublisher eventPublisher;
    private final SagaOrchestratorClient sagaClient;
    private final TransferValidationService validationService;
    private final IdempotencyService idempotencyService;
    private final RxJavaBridge bridge;

    @Override
    public Single<TransactionDocument> createTransaction(final UUID transactionId,
                                                         final String correlationId,
                                                         final TransactionCreateRequest request) {
        final Mono<TransactionDocument> pipeline = idempotencyService
                .tryAcquire(transactionId)
                .flatMap(acquired -> {
                    if (!acquired) {
                        return Mono.error(new IdempotencyConflictException(
                                "Transacción en curso o ya procesada: " + transactionId));
                    }
                    return transactionRepository.findByTransactionId(transactionId)
                            .flatMap(existing -> {
                                log.info("Idempotencia Mongo: tx {} ya existe (status={})",
                                        transactionId, existing.getStatus());
                                return Mono.just(existing);
                            })
                            .switchIfEmpty(Mono.defer(() -> processNew(
                                    transactionId, correlationId, request)));
                })
                .onErrorResume(err -> idempotencyService.release(transactionId)
                        .onErrorResume(releaseErr -> {
                            log.warn("Error liberando idempotency {}",
                                    transactionId, releaseErr);
                            return Mono.empty();
                        })
                        .then(Mono.error(err)))
                .flatMap(result -> idempotencyService.release(transactionId)
                        .onErrorResume(releaseErr -> {
                            log.warn("Error liberando idempotency {}",
                                    transactionId, releaseErr);
                            return Mono.empty();
                        })
                        .thenReturn(result));

        return bridge.toSingle(pipeline);
    }

    private Mono<TransactionDocument> processNew(final UUID transactionId,
                                                 final String correlationId,
                                                 final TransactionCreateRequest request) {
        if (!(request instanceof AccountTransferTransaction transfer)) {
            return persistPendingAndScheduleSaga(transactionId, correlationId, request);
        }
        // Validaciones síncronas de campos
        validationService.validateRequest(transfer);
        // Validación síncrona contra ms-account-service (rechazo temprano)
        return sagaClient.getAccount(transfer.getSourceAccountId(), correlationId)
                .zipWith(sagaClient.getAccount(transfer.getTargetAccountId(), correlationId))
                .flatMap(tuple -> {
                    final AccountDto source = tuple.getT1();
                    final AccountDto target = tuple.getT2();
                    validationService.validateAccounts(transfer, source, target);
                    return persistPendingAndScheduleSaga(
                            transactionId, correlationId, request);
                });
    }

    private Mono<TransactionDocument> persistPendingAndScheduleSaga(
            final UUID transactionId,
            final String correlationId,
            final TransactionCreateRequest request) {

        final TransactionDocument tx = buildPendingTransaction(transactionId, request);

        return transactionRepository.save(tx)
                .flatMap(saved -> eventPublisher
                        .publishStateEvent(saved, "TransactionAccepted", correlationId)
                        .onErrorResume(e -> {
                            log.warn("No se pudo publicar TransactionAccepted para {}: {}",
                                    saved.getId(), e.getMessage());
                            return Mono.empty();
                        })
                        .thenReturn(saved))
                .doOnNext(saved -> executeSagaAsync(saved, request, correlationId))
                .doOnError(e -> log.error("Error persistiendo transacción {}",
                        transactionId, e));
    }

    private void executeSagaAsync(final TransactionDocument tx,
                                  final TransactionCreateRequest request,
                                  final String correlationId) {
        executeSaga(tx, request, correlationId)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        done -> log.info("SAGA finalizada tx={} status={}",
                                done.getId(), done.getStatus()),
                        err -> log.error("SAGA falló tx={}", tx.getId(), err));
    }

    private Mono<TransactionDocument> executeSaga(final TransactionDocument tx,
                                                  final TransactionCreateRequest request,
                                                  final String corrId) {
        if (request instanceof AccountTransferTransaction t) {
            return executeAccountTransferSaga(tx, t, corrId);
        } else if (request instanceof CreditPaymentTransaction t) {
            return executeCreditPaymentSaga(tx, t, corrId);
        } else if (request instanceof CardConsumptionTransaction t) {
            return executeCardConsumptionSaga(tx, t, corrId);
        } else if (request instanceof WalletTransferTransaction t) {
            return executeWalletTransferSaga(tx, t, corrId);
        }
        return Mono.error(new InvalidTransactionException(
                "Tipo de transacción no soportado"));
    }

    private Mono<TransactionDocument> executeAccountTransferSaga(
            final TransactionDocument tx,
            final AccountTransferTransaction req,
            final String corrId) {
        return sagaClient.accountTransfer(
                        req.getSourceAccountId(),
                        req.getTargetAccountId(),
                        tx.getAmount(),
                        tx.getCurrency(),
                        req.getDescription(),
                        corrId,
                        tx.getId())
                .then(markCompletedAndPublish(tx, corrId))
                .onErrorResume(e -> {
                    log.error("Fallo transferencia. Transacción fallida.", e);
                    return markFailedAndPublish(tx, e.getMessage(), corrId);
                });
    }

    private Mono<TransactionDocument> executeCreditPaymentSaga(
            final TransactionDocument tx,
            final CreditPaymentTransaction req,
            final String corrId) {
        return sagaClient.accountDebit(req.getSourceAccountId(), tx.getAmount(),
                        tx.getCurrency(), corrId)
                .then(Mono.defer(() -> sagaClient.creditApplyPayment(
                                req.getCreditId(), tx.getAmount(), tx.getCurrency(), corrId)
                        .then(markCompletedAndPublish(tx, corrId))
                        .onErrorResume(e -> {
                            log.error("Fallo pago crédito, compensando...", e);
                            return sagaClient.accountCompensateDebit(
                                            req.getSourceAccountId(), tx.getAmount(),
                                            tx.getCurrency(), corrId)
                                    .then(markCompensatedAndPublish(
                                            tx, e.getMessage(), corrId));
                        })))
                .onErrorResume(e -> {
                    log.error("Fallo débito inicial. Transacción fallida.", e);
                    return markFailedAndPublish(tx, e.getMessage(), corrId);
                });
    }

    private Mono<TransactionDocument> executeCardConsumptionSaga(
            final TransactionDocument tx,
            final CardConsumptionTransaction req,
            final String corrId) {
        return sagaClient.cardConsume(req.getCardId(), tx.getAmount(),
                        req.getMerchant(), corrId)
                .then(markCompletedAndPublish(tx, corrId))
                .onErrorResume(e -> {
                    log.error("Fallo consumo tarjeta, compensando...", e);
                    return sagaClient.cardCompensateConsume(
                                    req.getCardId(), tx.getAmount(), corrId)
                            .then(markCompensatedAndPublish(tx, e.getMessage(), corrId));
                });
    }

    private Mono<TransactionDocument> executeWalletTransferSaga(
            final TransactionDocument tx,
            final WalletTransferTransaction req,
            final String corrId) {
        final String sourcePhone = "+51999888777";
        return sagaClient.walletDebit(sourcePhone, tx.getAmount(), tx.getCurrency(), corrId)
                .then(Mono.defer(() -> sagaClient.walletCredit(
                                req.getTargetPhoneNumber(), tx.getAmount(),
                                tx.getCurrency(), corrId)
                        .then(markCompletedAndPublish(tx, corrId))
                        .onErrorResume(e -> {
                            log.error("Fallo crédito wallet destino, compensando...", e);
                            return sagaClient.walletCompensateDebit(
                                            sourcePhone, tx.getAmount(),
                                            tx.getCurrency(), corrId)
                                    .then(markCompensatedAndPublish(
                                            tx, e.getMessage(), corrId));
                        })))
                .onErrorResume(e -> {
                    log.error("Fallo débito wallet origen. Transacción fallida.", e);
                    return markFailedAndPublish(tx, e.getMessage(), corrId);
                });
    }

    private Mono<TransactionDocument> markCompletedAndPublish(final TransactionDocument tx,
                                                              final String corrId) {
        tx.setStatus("COMPLETED");
        tx.setCompletedAt(LocalDateTime.now(ZoneOffset.UTC));
        return transactionRepository.save(tx)
                .flatMap(saved -> Mono.when(
                                eventPublisher.publishStateEvent(
                                        saved, "TransactionCompleted", corrId),
                                eventPublisher.publishTransferCompleted(
                                        saved, "TransactionCompleted", corrId))
                        .thenReturn(saved));
    }

    private Mono<TransactionDocument> markCompensatedAndPublish(final TransactionDocument tx,
                                                                final String reason,
                                                                final String corrId) {
        tx.setStatus("COMPENSATED");
        tx.setFailureReason(reason);
        return transactionRepository.save(tx)
                .flatMap(saved -> Mono.when(
                                eventPublisher.publishStateEvent(
                                        saved, "TransactionCompensated", corrId),
                                eventPublisher.publishTransferCompleted(
                                        saved, "TransactionCompensated", corrId))
                        .thenReturn(saved));
    }

    private Mono<TransactionDocument> markFailedAndPublish(final TransactionDocument tx,
                                                           final String reason,
                                                           final String corrId) {
        tx.setStatus("FAILED");
        tx.setFailureReason(reason);
        return transactionRepository.save(tx)
                .flatMap(saved -> Mono.when(
                                eventPublisher.publishStateEvent(
                                        saved, "TransactionFailed", corrId),
                                eventPublisher.publishTransferCompleted(
                                        saved, "TransactionFailed", corrId))
                        .thenReturn(saved));
    }

    private TransactionDocument buildPendingTransaction(final UUID txId,
                                                        final TransactionCreateRequest request) {
        if (request instanceof AccountTransferTransaction req) {
            return buildTx(txId, "ACCOUNT_TRANSFER", req.getAmount(), req.getCurrency(),
                    req.getSourceAccountId(), req.getTargetAccountId(),
                    null, null, req.getDescription());
        } else if (request instanceof CreditPaymentTransaction req) {
            return buildTx(txId, "CREDIT_PAYMENT", req.getAmount(), req.getCurrency(),
                    req.getSourceAccountId(), req.getCreditId(),
                    null, null, req.getDescription());
        } else if (request instanceof CardConsumptionTransaction req) {
            return buildTx(txId, "CARD_CONSUMPTION", req.getAmount(), req.getCurrency(),
                    req.getCardId(), null,
                    req.getMerchant(), null, req.getDescription());
        } else if (request instanceof WalletTransferTransaction req) {
            return buildTx(txId, "WALLET_TRANSFER", req.getAmount(), req.getCurrency(),
                    null, null, null,
                    req.getTargetPhoneNumber(), req.getDescription());
        }
        throw new InvalidTransactionException("Tipo inválido");
    }

    private TransactionDocument buildTx(final UUID txId, final String type,
                                        final String amount, final String currency,
                                        final UUID sourceId, final UUID targetId,
                                        final String merchant, final String targetPhone,
                                        final String desc) {
        return TransactionDocument.builder()
                .id(txId)
                .transactionId(txId)
                .type(type)
                .status("PENDING")
                .amount(new BigDecimal(amount))   // BigDecimal, nunca double
                .currency(currency)
                .sourceId(sourceId)
                .targetId(targetId)
                .merchant(merchant)
                .targetPhoneNumber(targetPhone)
                .description(desc)
                .createdAt(LocalDateTime.now(ZoneOffset.UTC))
                .build();
    }

    @Override
    public Single<TransactionDocument> findById(final UUID transactionId) {
        return bridge.toSingle(transactionRepository.findByTransactionId(transactionId)
                .switchIfEmpty(Mono.error(new TransactionNotFoundException(
                        "Transacción no encontrada: " + transactionId))));
    }
}