package com.banking.transaction.config;

import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.dto.CardConsumptionTransaction;
import com.banking.transaction.dto.CreditPaymentTransaction;
import com.banking.transaction.dto.TransactionCreateRequest;
import com.banking.transaction.dto.WalletTransferTransaction;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class TransactionRequestDeserializer extends StdDeserializer<TransactionCreateRequest> {

    public TransactionRequestDeserializer() {
        this(null);
    }

    public TransactionRequestDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public TransactionCreateRequest deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);
        String type = node.has("type") ? node.get("type").asText() : "";

        switch (type) {
            case "ACCOUNT_TRANSFER":
                return jp.getCodec().treeToValue(node, AccountTransferTransaction.class);
            case "CREDIT_PAYMENT":
                return jp.getCodec().treeToValue(node, CreditPaymentTransaction.class);
            case "CARD_CONSUMPTION":
                return jp.getCodec().treeToValue(node, CardConsumptionTransaction.class);
            case "WALLET_TRANSFER":
                return jp.getCodec().treeToValue(node, WalletTransferTransaction.class);
            default:
                return null; // O lanzar una excepción
        }
    }
}