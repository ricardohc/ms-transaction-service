package com.banking.transaction.client;

import com.banking.transaction.exception.AccountNotFoundException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaOrchestratorClient {

    private static final String CHANNEL_ID = "MS-TRANSACTION-SERVICE";

    private final WebClient.Builder webClientBuilder;

    @Value("${external.services.account.url}")
    private String accountUrl;

    @Value("${external.services.credit.url}")
    private String creditUrl;

    @Value("${external.services.card.url}")
    private String cardUrl;

    @Value("${external.services.wallet.url}")
    private String walletUrl;

    @CircuitBreaker(name = "account-cb")
    public Mono<AccountDto> getAccount(final UUID accountId, final String corrId) {
        return webClientBuilder.build().get()
                .uri(accountUrl + "/api/v1/accounts/{accountId}", accountId)
                .header("X-Correlation-ID", corrId)
                .header("X-Channel-ID", CHANNEL_ID)
                .header("Accept", "application/json")
                .retrieve()
                .onStatus(status -> status.value() == HttpStatus.NOT_FOUND.value(),
                        res -> res.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> {
                                    log.warn("Cuenta {} no encontrada. Body: {}",
                                            accountId, body);
                                    return Mono.error(new AccountNotFoundException(
                                            accountId.toString()));
                                }))
                .onStatus(HttpStatusCode::isError, res -> res.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .flatMap(body -> Mono.error(new RuntimeException(
                                "Error al obtener cuenta: " + body))))
                .bodyToMono(AccountDto.class)
                .switchIfEmpty(Mono.error(new AccountNotFoundException(
                        accountId.toString())));
    }

    @CircuitBreaker(name = "account-cb")
    public Mono<Void> accountTransfer(final UUID sourceAccountId,
                                      final UUID targetAccountId,
                                      final BigDecimal amount,
                                      final String currency,
                                      final String description,
                                      final String corrId,
                                      final UUID transactionId) {
        final Map<String, Object> body = new HashMap<>();
        body.put("sourceAccountId", sourceAccountId.toString());
        body.put("targetAccountId", targetAccountId.toString());
        body.put("amount", amount);
        body.put("currency", currency);
        if (description != null && !description.isBlank()) {
            body.put("description", description);
        }

        log.info("POST /api/v1/accounts/transfers body={}", body);

        return webClientBuilder.build().post()
                .uri(accountUrl + "/api/v1/accounts/transfers")
                .header("X-Correlation-ID", corrId)
                .header("X-Channel-ID", CHANNEL_ID)
                .header("X-Transaction-ID", transactionId.toString())
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .exchangeToMono(response -> {
                    final HttpStatusCode status = response.statusCode();
                    if (status.is2xxSuccessful()) {
                        return response.releaseBody();
                    }
                    return response.bodyToMono(String.class)
                            .defaultIfEmpty("<empty>")
                            .flatMap(err -> {
                                log.error("Fallo transferencia {}→{} status={} body={}",
                                        sourceAccountId, targetAccountId,
                                        status.value(), err);
                                return Mono.error(new RuntimeException(
                                        "Error en transferencia (HTTP "
                                                + status.value() + "): " + err));
                            });
                });
    }

    @CircuitBreaker(name = "account-cb")
    public Mono<Void> accountDebit(final UUID accountId, final BigDecimal amount,
                                   final String currency, final String corrId) {
        final Map<String, Object> body = new HashMap<>();
        body.put("type", "DEBIT");
        body.put("amount", amount);
        body.put("currency", currency);
        return postAccountOperation(accountId, body, corrId, "debitar");
    }

    @CircuitBreaker(name = "account-cb")
    public Mono<Void> accountCredit(final UUID accountId, final BigDecimal amount,
                                    final String currency, final String corrId) {
        final Map<String, Object> body = new HashMap<>();
        body.put("type", "CREDIT");
        body.put("amount", amount);
        body.put("currency", currency);
        return postAccountOperation(accountId, body, corrId, "acreditar");
    }

    @CircuitBreaker(name = "account-cb")
    public Mono<Void> accountCompensateDebit(final UUID accountId, final BigDecimal amount,
                                             final String currency, final String corrId) {
        return accountCredit(accountId, amount, currency, corrId);
    }

    private Mono<Void> postAccountOperation(final UUID accountId,
                                            final Map<String, Object> body,
                                            final String corrId,
                                            final String action) {
        return webClientBuilder.build().post()
                .uri(accountUrl + "/api/v1/accounts/{accountId}/transactions", accountId)
                .header("X-Correlation-ID", corrId)
                .header("X-Channel-ID", CHANNEL_ID)
                .header("X-Transaction-ID", UUID.randomUUID().toString())
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .retrieve()
                .onStatus(HttpStatusCode::isError, res -> res.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .flatMap(err -> Mono.error(new RuntimeException(
                                "Error al " + action + " cuenta: " + err))))
                .bodyToMono(Void.class);
    }

    // ==================== CREDIT ====================

    @CircuitBreaker(name = "account-cb")
    public Mono<Void> creditApplyPayment(final UUID creditId, final BigDecimal amount,
                                         final String currency, final String corrId) {
        final Map<String, Object> updateBody = new HashMap<>();
        updateBody.put("balance", amount.negate());

        return webClientBuilder.build().patch()
                .uri(creditUrl + "/api/v1/credits/{creditId}", creditId)
                .header("X-Correlation-ID", corrId)
                .header("X-Channel-ID", CHANNEL_ID)
                .header("X-Transaction-ID", UUID.randomUUID().toString())
                .header("Content-Type", "application/json")
                .bodyValue(updateBody)
                .retrieve()
                .onStatus(HttpStatusCode::isError, res -> res.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .flatMap(body -> Mono.error(new RuntimeException(
                                "Error al pagar crédito: " + body))))
                .bodyToMono(Void.class);
    }

    public Mono<Void> cardConsume(final UUID cardId, final BigDecimal amount,
                                  final String merchant, final String corrId) {
        final Map<String, Object> body = new HashMap<>();
        body.put("amount", amount);
        body.put("currency", "PEN");
        body.put("merchant", merchant);

        return webClientBuilder.build().patch()
                .uri(cardUrl + "/api/v1/cards/{cardId}/consumptions", cardId)
                .header("X-Correlation-ID", corrId)
                .header("X-Channel-ID", CHANNEL_ID)
                .header("X-Transaction-ID", UUID.randomUUID().toString())
                .header("Content-Type", "application/json")
                .bodyValue(body)
                .retrieve()
                .onStatus(HttpStatusCode::isError, res -> res.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .flatMap(err -> Mono.error(new RuntimeException(
                                "Error en consumo de tarjeta: " + err))))
                .bodyToMono(Void.class);
    }

    public Mono<Void> cardCompensateConsume(final UUID cardId, final BigDecimal amount,
                                            final String corrId) {
        return Mono.empty();
    }

    public Mono<Void> walletDebit(final String phoneNumber, final BigDecimal amount,
                                  final String currency, final String corrId) {
        return processWalletTransaction(phoneNumber, amount.negate(), corrId);
    }

    public Mono<Void> walletCredit(final String phoneNumber, final BigDecimal amount,
                                   final String currency, final String corrId) {
        return processWalletTransaction(phoneNumber, amount, corrId);
    }

    public Mono<Void> walletCompensateDebit(final String phoneNumber, final BigDecimal amount,
                                            final String currency, final String corrId) {
        return processWalletTransaction(phoneNumber, amount, corrId);
    }

    private Mono<Void> processWalletTransaction(final String phoneNumber,
                                                final BigDecimal amountDelta,
                                                final String corrId) {
        return Mono.empty();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccountDto {
        private String id;
        private String accountNumber;
        private String customerId;
        private String accountType;
        private String profile;
        private String status;
        private BigDecimal balance;
        private String currency;
        private String createdAt;
    }
}