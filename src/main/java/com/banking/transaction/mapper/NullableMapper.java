package com.banking.transaction.mapper;

import java.util.UUID;
import org.openapitools.jackson.nullable.JsonNullable;

public final class NullableMapper {

    private NullableMapper() {
    }

    public static JsonNullable<UUID> of(final UUID value) {
        return value == null ? JsonNullable.undefined() : JsonNullable.of(value);
    }
}
