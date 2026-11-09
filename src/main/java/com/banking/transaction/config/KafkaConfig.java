package com.banking.transaction.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

/** Reactive Kafka producer configuration. */
@Configuration
public class KafkaConfig {

    @Bean
    public ReactiveKafkaProducerTemplate<String, Object> reactiveKafkaProducerTemplate(
            final KafkaProperties properties) {
        return new ReactiveKafkaProducerTemplate<>(
                SenderOptions.create(properties.buildProducerProperties()));
    }
}
