package com.dataplatform.processor.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(new JavaTimeModule());

    private JsonUtils() {}

    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    public static <T> T deserialize(InputStream stream, Class<T> clazz) {
        try {
            return MAPPER.readValue(stream, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserializeFile(File file, Class<T> clazz) {
        try (var stream = new FileInputStream(file)) {
            return MAPPER.readValue(stream, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
