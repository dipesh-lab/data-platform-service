package com.dataplatform.processor.consumers.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
@JsonIgnoreProperties(ignoreUnknown = true)
public record StoredData(
        String namespace,
        String type,
        String location,
        int totalRecords,
        long length,
        List<String> childItems) {
}
