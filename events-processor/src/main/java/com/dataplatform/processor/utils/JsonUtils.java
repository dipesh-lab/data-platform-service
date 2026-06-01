package com.dataplatform.processor.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;

public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .registerModule(new JavaTimeModule());

    private JsonUtils() {}

    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    public static <T> T deserialize(InputStream stream, Class<T> clazz) throws IOException {
        return MAPPER.readValue(stream, clazz);
    }

}
