package com.banking.transaction.service;

import io.reactivex.rxjava3.core.Single;
import reactor.core.publisher.Mono;
import reactor.adapter.rxjava.RxJava3Adapter;

public class RxJavaBridge {

    public <T> Single<T> toSingle(final Mono<T> mono) {
        return RxJava3Adapter.monoToSingle(mono);
    }

    public <T> Mono<T> toMono(final Single<T> single) {
        return RxJava3Adapter.singleToMono(single);
    }
}
