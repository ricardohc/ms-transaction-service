package com.banking.transaction.service.impl;

import com.banking.transaction.document.OutboxDocument;
import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.dto.CardConsumptionTransaction;
import com.banking.transaction.dto.CreditPaymentTransaction;
import com.banking.transaction.dto.TransactionCreateRequest;
import com.banking.transaction.dto.WalletTransferTransaction;
import com.banking.transaction.event.TransactionEventPublisher;
import com.banking.transaction.exception.InvalidTransactionException;
import com.banking.transaction.exception.TransactionNotFoundException;
import com.banking.transaction.repository.OutboxRepository;
import com.banking.transaction.repository.TransactionRepository;
import com.banking.transaction.service.TransactionService;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Orchestrator implementation. Steps 2-4 of the reference flow: idempotency,
 * atomic PENDING + outbox persistence and async publication.
 * <p>
 * Reactive bridge (documented per common-rules): the controller path is
 * WebFlux/Reactor (generated API returns Mono). The orchestration pipeline is
 * RxJava 3; the interop point is {@link RxJavaBridge}, converting the Single
 * pipeline to a Mono via RxJava3Adapter, and back to RxJava with
 * {@code Mono#to} conversions. No blockingGet() is used.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionServiceImpl implements TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final TransactionEventPublisher eventPublisher;
    private final RxJavaBridge bridge;

    @Override
    public Single<TransactionDocument> createTransaction(final UUID transactionId,
            final String correlationId, final TransactionCreateRequest request) {
        return bridge.toSingle(
                transactionRepository.findByTransactionId(transactionId)
                        .flatMap(existing -> {
                            log.debug("Idempotent replay for transactionId={}", transactionId);
                            return Mono.just(existing);
                        })
                        .switchIfEmpty(buildAndPersist(transactionId, correlationId, request)))
                .doOnSuccess(tx -> eventPublisher.publishAccepted(tx, correlationId));
    }

    @Override
    public Single<TransactionDocument> findById(final UUID transactionId) {
        return bridge.toSingle(transactionRepository.findById(transactionId)
                        .switchIfEmpty(Mono.error(
                                new TransactionNotFoundException(
                                        "Transaction not found: " + transactionId))));
    }

    private Mono<TransactionDocument> buildAndPersist(final UUID transactionId,
            final String correlationId, final TransactionCreateRequest request) {
        return Mono.defer(() -> {
            final TransactionDocument tx = fromRequest(transactionId, request);
            final OutboxDocument outbox = OutboxDocument.builder()
                    .id(UUID.randomUUID())
                    .eventId(UUID.randomUUID().toString())
                    .transactionId(transactionId)
                    .eventType("TransactionAccepted")
                    .topic(TransactionEventPublisher.TOPIC)
                    .aggregateId(transactionId.toString())
                    .aggregateType("Transaction")
                    .producer("ms-transaction-service")
                    .occurredAt(nowUtc())
                    .correlationId(correlationId)
                    .payloadJson("{\"transactionId\":\"" + transactionId + "\"}")
                    .status("PENDING")
                    .build();
            // Atomic transaction + outbox write (requires replica set).
            return transactionRepository.save(tx)
                    .flatMap(saved -> outboxRepository.save(outbox).thenReturn(saved));
        });
    }

    private TransactionDocument fromRequest(final UUID transactionId,
            final TransactionCreateRequest request) {
        if (request instanceof AccountTransferTransaction accountTransfer) {
            validateAmount(accountTransfer.getAmount());
            return TransactionDocument.builder()
                    .id(transactionId)
                    .transactionId(transactionId)
                    .type("ACCOUNT_TRANSFER")
                    .status("PENDING")
                    .amount(new BigDecimal(accountTransfer.getAmount()))
                    .currency(accountTransfer.getCurrency())
                    .sourceId(accountTransfer.getSourceAccountId())
                    .targetId(accountTransfer.getTargetAccountId())
                    .description(accountTransfer.getDescription())
                    .createdAt(nowUtc())
                    .build();
        }
        if (request instanceof CreditPaymentTransaction creditPayment) {
            validateAmount(creditPayment.getAmount());
            return TransactionDocument.builder()
                    .id(transactionId)
                    .transactionId(transactionId)
                    .type("CREDIT_PAYMENT")
                    .status("PENDING")
                    .amount(new BigDecimal(creditPayment.getAmount()))
                    .currency(creditPayment.getCurrency())
                    .creditId(creditPayment.getCreditId())
                    .sourceId(creditPayment.getSourceAccountId())
                    .description(creditPayment.getDescription())
                    .createdAt(nowUtc())
                    .build();
        }
        if (request instanceof CardConsumptionTransaction cardConsumption) {
            validateAmount(cardConsumption.getAmount());
            return TransactionDocument.builder()
                    .id(transactionId)
                    .transactionId(transactionId)
                    .type("CARD_CONSUMPTION")
                    .status("PENDING")
                    .amount(new BigDecimal(cardConsumption.getAmount()))
                    .currency(cardConsumption.getCurrency())
                    .cardId(cardConsumption.getCardId())
                    .merchant(cardConsumption.getMerchant())
                    .description(cardConsumption.getDescription())
                    .createdAt(nowUtc())
                    .build();
        }
        if (request instanceof WalletTransferTransaction walletTransfer) {
            validateAmount(walletTransfer.getAmount());
            // Source wallet is never accepted from the body; derived from JWT.
            return TransactionDocument.builder()
                    .id(transactionId)
                    .transactionId(transactionId)
                    .type("WALLET_TRANSFER")
                    .status("PENDING")
                    .amount(new BigDecimal(walletTransfer.getAmount()))
                    .currency(walletTransfer.getCurrency())
                    .targetPhoneNumber(walletTransfer.getTargetPhoneNumber())
                    .description(walletTransfer.getDescription())
                    .createdAt(nowUtc())
                    .build();
        }
        throw new InvalidTransactionException("Unsupported transaction type");
    }

    private void validateAmount(final String amount) {
        if (amount == null || amount.isBlank()) {
            throw new InvalidTransactionException("Amount is required");
        }
        try {
            final BigDecimal value = new BigDecimal(amount);
            if (value.signum() <= 0) {
                throw new InvalidTransactionException("Amount must be greater than zero");
            }
        } catch (NumberFormatException ex) {
            throw new InvalidTransactionException("Invalid amount format");
        }
    }

    private static LocalDateTime nowUtc() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }
}
