package com.banking.transaction.exception;

import com.banking.transaction.dto.ErrorResponse;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

/** Global error handler mapping domain exceptions to contract HTTP codes. */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    /** 404 - transaction not found. */
    @ExceptionHandler(TransactionNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(
            final TransactionNotFoundException ex, final ServerWebExchange exchange) {
        return build(HttpStatus.NOT_FOUND, "TRANSACTION_NOT_FOUND", ex.getMessage(), exchange);
    }

    /** 422 - invalid transaction payload. */
    @ExceptionHandler(InvalidTransactionException.class)
    public ResponseEntity<ErrorResponse> handleInvalid(
            final InvalidTransactionException ex, final ServerWebExchange exchange) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "INVALID_TRANSACTION",
                ex.getMessage(), exchange);
    }

    /** 500 - unexpected errors. */
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
