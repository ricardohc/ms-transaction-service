package com.banking.transaction.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.banking.transaction.client.SagaOrchestratorClient;
import com.banking.transaction.client.SagaOrchestratorClient.AccountDto;
import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.dto.CardConsumptionTransaction;
import com.banking.transaction.event.TransactionEventPublisher;
import com.banking.transaction.exception.BusinessValidationException;
import com.banking.transaction.exception.IdempotencyConflictException;
import com.banking.transaction.exception.TransactionNotFoundException;
import com.banking.transaction.repository.TransactionRepository;
import com.banking.transaction.service.IdempotencyService;
import com.banking.transaction.service.RxJavaBridge;
import com.banking.transaction.service.TransactionServiceImpl;
import com.banking.transaction.service.TransferValidationService;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

class TransactionServiceImplTest {

    private TransactionRepository repository;
    private TransactionEventPublisher eventPublisher;
    private SagaOrchestratorClient sagaClient;
    private TransferValidationService validationService;
    private IdempotencyService idempotencyService;
    private TransactionServiceImpl service;

    private final UUID txId = UUID.randomUUID();
    private final UUID sourceId = UUID.randomUUID();
    private final UUID targetId = UUID.randomUUID();
    private final String corrId = "corr-1";

    private final List<TransactionDocument> savedSnapshots = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        repository = Mockito.mock(TransactionRepository.class);
        eventPublisher = Mockito.mock(TransactionEventPublisher.class);
        sagaClient = Mockito.mock(SagaOrchestratorClient.class);
        validationService = Mockito.mock(TransferValidationService.class);
        idempotencyService = Mockito.mock(IdempotencyService.class);

        when(eventPublisher.publishStateEvent(any(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(eventPublisher.publishTransferCompleted(any(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(idempotencyService.release(any())).thenReturn(Mono.empty());

        savedSnapshots.clear();
        when(repository.save(any(TransactionDocument.class))).thenAnswer(inv -> {
            TransactionDocument doc = inv.getArgument(0);
            savedSnapshots.add(snapshot(doc));
            return Mono.just(doc);
        });

        service = new TransactionServiceImpl(repository, eventPublisher, sagaClient,
                validationService, idempotencyService, new RxJavaBridge());
    }

    private TransactionDocument snapshot(TransactionDocument doc) {
        return TransactionDocument.builder()
                .id(doc.getId())
                .transactionId(doc.getTransactionId())
                .type(doc.getType())
                .status(doc.getStatus())
                .amount(doc.getAmount())
                .currency(doc.getCurrency())
                .sourceId(doc.getSourceId())
                .targetId(doc.getTargetId())
                .merchant(doc.getMerchant())
                .targetPhoneNumber(doc.getTargetPhoneNumber())
                .description(doc.getDescription())
                .createdAt(doc.getCreatedAt())
                .build();
    }

    private AccountTransferTransaction transferRequest() {
        AccountTransferTransaction req = new AccountTransferTransaction();
        req.setAmount("100.00");
        req.setCurrency("PEN");
        req.setSourceAccountId(sourceId);
        req.setTargetAccountId(targetId);
        req.setDescription("test");
        return req;
    }

    private AccountDto account(String balance) {
        AccountDto dto = new AccountDto();
        dto.setId(UUID.randomUUID().toString());
        dto.setAccountType("SAVINGS");
        dto.setBalance(new BigDecimal(balance));
        dto.setCurrency("PEN");
        dto.setStatus("ACTIVE");
        return dto;
    }

    @Test
    void createTransactionRejectsDuplicateIdempotency() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(false));

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertError(IdempotencyConflictException.class);

        verify(repository, never()).save(any());
    }

    @Test
    void createTransactionReturnsExistingWhenMongoAlreadyHasIt() {
        TransactionDocument existing = TransactionDocument.builder()
                .id(txId).transactionId(txId).status("COMPLETED").build();

        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.just(existing));

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertValue(v -> "COMPLETED".equals(v.getStatus()))
                .assertComplete();

        verify(repository, never()).save(any());
    }

    @Test
    void createTransactionFailsWhenTransferValidationFails() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        Mockito.doThrow(new BusinessValidationException("bad amount"))
                .when(validationService).validateRequest(any(AccountTransferTransaction.class));

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertError(BusinessValidationException.class);

        verify(repository, never()).save(any());
    }

    @Test
    void createTransactionFailsWhenAccountDoesNotExist() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.getAccount(any(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("account not found")));

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertError(RuntimeException.class);

        verify(repository, never()).save(any());
    }

    @Test
    void createTransactionPersistsPendingAndReturnsIt() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.getAccount(sourceId, corrId)).thenReturn(Mono.just(account("1000.00")));
        when(sagaClient.getAccount(targetId, corrId)).thenReturn(Mono.just(account("500.00")));
        when(sagaClient.accountTransfer(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertValue(tx -> txId.equals(tx.getTransactionId()))
                .assertComplete();

        org.junit.jupiter.api.Assertions.assertTrue(savedSnapshots.size() >= 1);
        TransactionDocument pending = savedSnapshots.get(0);
        org.junit.jupiter.api.Assertions.assertEquals("PENDING", pending.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals("ACCOUNT_TRANSFER", pending.getType());
        org.junit.jupiter.api.Assertions.assertEquals(new BigDecimal("100.00"), pending.getAmount());
        org.junit.jupiter.api.Assertions.assertEquals("PEN", pending.getCurrency());

        verify(eventPublisher, times(1))
                .publishStateEvent(any(), eq("TransactionAccepted"), eq(corrId));
    }

    @Test
    void createCardConsumptionPersistsPendingWithoutAccountLookup() {
        CardConsumptionTransaction req = new CardConsumptionTransaction();
        req.setAmount("25.00");
        req.setCurrency("PEN");
        req.setCardId(UUID.randomUUID());
        req.setMerchant("Shop");
        req.setDescription("buy");

        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.cardConsume(any(), any(), any(), any())).thenReturn(Mono.empty());

        service.createTransaction(txId, corrId, req)
                .test()
                .assertValue(tx -> txId.equals(tx.getTransactionId()))
                .assertComplete();

        org.junit.jupiter.api.Assertions.assertTrue(savedSnapshots.size() >= 1);
        TransactionDocument pending = savedSnapshots.get(0);
        org.junit.jupiter.api.Assertions.assertEquals("PENDING", pending.getStatus());
        org.junit.jupiter.api.Assertions.assertEquals("CARD_CONSUMPTION", pending.getType());

        verify(sagaClient, never()).getAccount(any(), anyString());
    }

    @Test
    void findByIdReturnsExistingTransaction() {
        TransactionDocument existing = TransactionDocument.builder()
                .id(txId).transactionId(txId).status("COMPLETED").build();
        when(repository.findByTransactionId(txId)).thenReturn(Mono.just(existing));

        service.findById(txId)
                .test()
                .assertValue(v -> "COMPLETED".equals(v.getStatus()))
                .assertComplete();
    }

    @Test
    void findByIdFailsWhenTransactionDoesNotExist() {
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());

        service.findById(txId)
                .test()
                .assertError(TransactionNotFoundException.class);
    }

    @Test
    void idempotencyReleasedOnSuccess() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.getAccount(sourceId, corrId)).thenReturn(Mono.just(account("1000.00")));
        when(sagaClient.getAccount(targetId, corrId)).thenReturn(Mono.just(account("500.00")));
        when(sagaClient.accountTransfer(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertComplete();

        verify(idempotencyService, times(1)).release(txId);
    }

    @Test
    void idempotencyReleasedOnError() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.getAccount(any(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("boom")));

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertError(RuntimeException.class);

        verify(idempotencyService, times(1)).release(txId);
    }

    @Test
    void transactionAcceptedEventFailureIsToleratedOnCreate() {
        when(idempotencyService.tryAcquire(txId)).thenReturn(Mono.just(true));
        when(repository.findByTransactionId(txId)).thenReturn(Mono.empty());
        when(sagaClient.getAccount(sourceId, corrId)).thenReturn(Mono.just(account("1000.00")));
        when(sagaClient.getAccount(targetId, corrId)).thenReturn(Mono.just(account("500.00")));
        when(eventPublisher.publishStateEvent(any(), anyString(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("kafka down")));
        when(sagaClient.accountTransfer(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Mono.empty());

        service.createTransaction(txId, corrId, transferRequest())
                .test()
                .assertValue(tx -> txId.equals(tx.getTransactionId()))
                .assertComplete();

        org.junit.jupiter.api.Assertions.assertTrue(savedSnapshots.size() >= 1);
        org.junit.jupiter.api.Assertions.assertEquals("PENDING",
                savedSnapshots.get(0).getStatus());
    }
}