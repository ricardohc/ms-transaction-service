package com.banking.transaction.config;

import com.banking.transaction.dto.TransactionCreateRequest;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public SimpleModule transactionRequestModule() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(TransactionCreateRequest.class, new TransactionRequestDeserializer());
        return module;
    }
}