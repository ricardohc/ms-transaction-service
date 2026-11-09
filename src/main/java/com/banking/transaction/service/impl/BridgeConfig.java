package com.banking.transaction.service.impl;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Provides the RxJava/Reactor bridge as a Spring bean. */
@Configuration
public class BridgeConfig {

    @Bean
    public RxJavaBridge rxJavaBridge() {
        return new RxJavaBridge();
    }
}
