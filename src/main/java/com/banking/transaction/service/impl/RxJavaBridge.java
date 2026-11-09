package com.banking.transaction.service.impl;

import io.reactivex.rxjava3.core.Single;
import reactor.core.publisher.Mono;
import reactor.adapter.rxjava.RxJava3Adapter;

/**
 * Documented interop point between RxJava 3 (orchestration pipelines) and
 * Reactor (WebFlux controllers/repositories), per common-rules.md.
 * Uses reactor-adapter's RxJava3Adapter; never blocks.
 */
public class RxJavaBridge {

    /** Converts a Reactor Mono into an RxJava 3 Single. */
    public <T> Single<T> toSingle(final Mono<T> mono) {
        return RxJava3Adapter.monoToSingle(mono);
    }

    /** Converts an RxJava 3 Single into a Reactor Mono. */
    public <T> Mono<T> toMono(final Single<T> single) {
        return RxJava3Adapter.singleToMono(single);
    }
}
