package com.dataplatform.processor.consumers.models;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record StoredRawData(String namespace, String type, String location, int totalRecords, long length) {
}
