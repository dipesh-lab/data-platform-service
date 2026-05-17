package com.dataplatform.processor.consumers.models;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record StoredRawData(String location, String type, int totalRecords, long length) {
}
