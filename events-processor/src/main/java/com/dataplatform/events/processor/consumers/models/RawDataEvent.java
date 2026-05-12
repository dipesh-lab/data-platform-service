package com.dataplatform.events.processor.consumers.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;
import java.util.Map;

@Serdeable
public record RawDataEvent(String namespace,
                           String type,
                           String tenantId,
                           @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC") Instant eventTime,
                           Map<String, Object> attributes) {
}