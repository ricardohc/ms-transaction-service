package com.banking.transaction.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient.Builder webClientBuilder() {
        return WebClient.builder()
                .filter((request, next) ->
                        ReactiveSecurityContextHolder.getContext()
                                .map(ctx -> ctx.getAuthentication())
                                .filter(auth -> auth != null
                                        && auth.getCredentials() != null
                                        && !auth.getCredentials().toString().isBlank())
                                .map(Authentication::getCredentials)
                                .map(Object::toString)
                                .flatMap(token -> next.exchange(
                                        ClientRequest.from(request)
                                                .header("Authorization", "Bearer " + token)
                                                .build()))
                                .switchIfEmpty(Mono.defer(() -> next.exchange(request))));
    }
}