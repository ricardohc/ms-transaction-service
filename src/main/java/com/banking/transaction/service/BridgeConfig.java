package com.banking.transaction.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BridgeConfig {

    @Bean
    public RxJavaBridge rxJavaBridge() {
        return new RxJavaBridge();
    }
}
