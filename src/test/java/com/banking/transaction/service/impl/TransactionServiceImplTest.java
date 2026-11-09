package com.banking.transaction.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.banking.transaction.document.OutboxDocument;
import com.banking.transaction.document.TransactionDocument;
import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.dto.WalletTransferTransaction;
import com.banking.transaction.event.TransactionEventPublisher;
import com.banking.transaction.exception.InvalidTransactionException;
import com.banking.transaction.exception.TransactionNotFoundException;
import com.banking.transaction.repository.OutboxRepository;
import com.banking.transaction.repository.TransactionRepository;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

/** Unit tests for orchestration business rules using RxJava TestObserver. */
@ExtendWith(MockitoExtension.class)
class TransactionServiceImplTest {

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private TransactionEventPublisher eventPublisher;

    private TransactionServiceImpl service;

    private UUID transactionId;

    @BeforeEach
    void setUp() {
        service = new TransactionServiceImpl(transactionRepository, outboxRepository,
                eventPublisher, new RxJavaBridge());
        transactionId = UUID.randomUUID();
        lenient().when(transactionRepository.save(any(TransactionDocument.class)))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        lenient().when(outboxRepository.save(any(OutboxDocument.class)))
                .thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        lenient().when(transactionRepository.findByTransactionId(any(UUID.class)))
                .thenReturn(Mono.empty());
        lenient().when(transactionRepository.findById(any(UUID.class)))
                .thenReturn(Mono.empty());
    }

    private AccountTransferTransaction accountTransfer(final String amount) {
        final AccountTransferTransaction req = new AccountTransferTransaction();
        req.setType(AccountTransferTransaction.TypeEnum.ACCOUNT_TRANSFER);
        req.setAmount(amount);
        req.setCurrency("PEN");
        req.setSourceAccountId(UUID.randomUUID());
        req.setTargetAccountId(UUID.randomUUID());
        return req;
    }

    @Test
    void createsTransactionAndOutboxAtomically() {
        service.createTransaction(transactionId, "corr-1", accountTransfer("100.00"))
                .test()
                .assertValue(tx -> "PENDING".equals(tx.getStatus())
                        && new BigDecimal("100.00").compareTo(tx.getAmount()) == 0)
                .assertComplete();
    }

    @Test
    void rejectsNonPositiveAmount() {
        service.createTransaction(transactionId, "corr-1", accountTransfer("0.00"))
                .test()
                .assertError(InvalidTransactionException.class);
    }

    @Test
    void isIdempotentByTransactionId() {
        final TransactionDocument existing = TransactionDocument.builder()
                .id(transactionId)
                .transactionId(transactionId)
                .status("PENDING")
                .build();
        when(transactionRepository.findByTransactionId(transactionId))
                .thenReturn(Mono.just(existing));

        service.createTransaction(transactionId, "corr-1", accountTransfer("50.00"))
                .test()
                .assertValue(tx -> tx.getId().equals(transactionId))
                .assertComplete();
    }

    @Test
    void findByIdReturnsErrorWhenMissing() {
        service.findById(transactionId)
                .test()
                .assertError(TransactionNotFoundException.class);
    }

    @Test
    void findByIdReturnsExisting() {
        final TransactionDocument existing = TransactionDocument.builder()
                .id(transactionId)
                .transactionId(transactionId)
                .status("PROCESSING")
                .build();
        when(transactionRepository.findById(transactionId)).thenReturn(Mono.just(existing));

        service.findById(transactionId)
                .test()
                .assertValue(tx -> "PROCESSING".equals(tx.getStatus()))
                .assertComplete();
    }

    @Test
    void walletTransferNeverStoresSourceFromClient() {
        final WalletTransferTransaction req = new WalletTransferTransaction();
        req.setType(WalletTransferTransaction.TypeEnum.WALLET_TRANSFER);
        req.setAmount("20.00");
        req.setCurrency("PEN");
        req.setTargetPhoneNumber("+51999888777");

        service.createTransaction(transactionId, "corr-1", req)
                .test()
                .assertValue(tx -> tx.getSourceWalletPhoneNumber() == null
                        && "+51999888777".equals(tx.getTargetPhoneNumber()))
                .assertComplete();
    }
}
