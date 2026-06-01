package com.dataplatform.processor.consumers.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;
import java.util.Map;

@Serdeable
public record DataEvent(String namespace,
                        String type,
                        String tenantId,
                        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC") Instant eventTime,
                        Map<String, Object> attributes) {
}