package com.banking.transaction.service;

import java.time.Duration;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final Duration TTL = Duration.ofSeconds(60);
    private static final String PREFIX = "tx:idempotency:";

    private final ReactiveStringRedisTemplate redis;

    public Mono<Boolean> tryAcquire(final UUID transactionId) {
        final String key = PREFIX + transactionId;
        return redis.opsForValue()
                .setIfAbsent(key, "PENDING", TTL)
                .doOnNext(acquired -> log.info("Idempotency key {} acquired={}", key, acquired))
                .onErrorResume(e -> {
                    log.warn("Redis no disponible, degradando idempotencia a solo Mongo: {}",
                            e.getMessage());
                    return Mono.just(true);
                });
    }

    public Mono<Void> release(final UUID transactionId) {
        return redis.delete(PREFIX + transactionId)
                .onErrorResume(e -> {
                    log.warn("No se pudo liberar idempotency key {}: {}",
                            transactionId, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}