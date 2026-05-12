package com.dataplatform.events.producer.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.Map;

@Serdeable
@Builder
public record DataEvent(String namespace,
                        String type,
                        String tenantId,
                        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC") Instant eventTime,
                        Map<String, Object> attributes) {

}