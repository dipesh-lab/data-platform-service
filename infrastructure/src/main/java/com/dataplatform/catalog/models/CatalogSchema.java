package com.dataplatform.catalog.models;

import java.util.List;

public record CatalogSchema(String tableName, String bucketName, PartitionType partitionType, List<Field> fields) {

    public enum PartitionType {
        DAY, HOUR;
    }

    public record Field(String name, DataType dataType){}

    public enum DataType {
        STRING, INT, LONG, TIMESTAMP, DATE;
    }
}
