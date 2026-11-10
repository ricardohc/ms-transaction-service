package com.banking.transaction.exception;

import com.banking.transaction.dto.ErrorResponse;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(TransactionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(
            final TransactionNotFoundException ex, final ServerWebExchange exchange) {
        return build(HttpStatus.NOT_FOUND, "TRANSACTION_NOT_FOUND", ex.getMessage(), exchange);
    }

    @ExceptionHandler(AccountNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleAccountNotFound(
            final AccountNotFoundException ex, final ServerWebExchange exchange) {
        log.warn("Account not found: {}", ex.getMessage());
        return build(HttpStatus.NOT_FOUND, "ACCOUNT_NOT_FOUND", ex.getMessage(), exchange);
    }

    @ExceptionHandler(BusinessValidationException.class)
    public ResponseEntity<ErrorResponse> handleBusiness(
            final BusinessValidationException ex, final ServerWebExchange exchange) {
        log.warn("Business validation failed: {}", ex.getMessage());
        return build(HttpStatus.BAD_REQUEST, "BUSINESS_VALIDATION_ERROR",
                ex.getMessage(), exchange);
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotency(
            final IdempotencyConflictException ex, final ServerWebExchange exchange) {
        log.warn("Idempotency conflict: {}", ex.getMessage());
        return build(HttpStatus.CONFLICT, "IDEMPOTENCY_CONFLICT",
                ex.getMessage(), exchange);
    }

    @ExceptionHandler(InvalidTransactionException.class)
    public ResponseEntity<ErrorResponse> handleInvalid(
            final InvalidTransactionException ex, final ServerWebExchange exchange) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "INVALID_TRANSACTION",
                ex.getMessage(), exchange);
    }

    @ExceptionHandler(CallNotPermittedException.class)
    public ResponseEntity<ErrorResponse> handleCircuitOpen(
            final CallNotPermittedException ex,
            final ServerWebExchange exchange) {
        log.error("Circuit breaker open: {}", ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "CIRCUIT_OPEN",
                "Servicio de cuentas temporalmente no disponible", exchange);
    }

    @ExceptionHandler(RedisConnectionFailureException.class)
    public ResponseEntity<ErrorResponse> handleRedisDown(
            final RedisConnectionFailureException ex, final ServerWebExchange exchange) {
        log.error("Redis unavailable", ex);
        return build(HttpStatus.SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE",
                "Servicio de idempotencia temporalmente no disponible", exchange);
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ErrorResponse> handleDataAccess(
            final DataAccessException ex, final ServerWebExchange exchange) {
        log.error("Data access error", ex);
        return build(HttpStatus.SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE",
                "Servicio de persistencia temporalmente no disponible", exchange);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(
            final Exception ex, final ServerWebExchange exchange) {
        log.error("Unhandled error", ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Unexpected error", exchange);
    }

    private ResponseEntity<ErrorResponse> build(final HttpStatus status, final String code,
                                                final String message, final ServerWebExchange exchange) {
        final ErrorResponse error = new ErrorResponse();
        error.setCode(code);
        error.setMessage(message);
        error.setTimestamp(OffsetDateTime.now());
        error.setCorrelationId(exchange.getRequest().getHeaders()
                .getFirst("X-Correlation-ID"));
        error.setDetails(List.of());
        return ResponseEntity.status(status).body(error);
    }
}