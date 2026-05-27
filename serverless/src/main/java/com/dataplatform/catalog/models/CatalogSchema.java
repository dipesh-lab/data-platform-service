package com.dataplatform.catalog.models;

import java.util.List;

public record CatalogSchema(String namespace, String tableName, String bucketName, PartitionType partitionType, List<Field> fields) {

    public enum PartitionType {
        DAY, HOUR;
    }

    public record Field(Integer id, String name, DataType dataType, Boolean deprecated){}

    public enum DataType {
        STRING, INT, LONG, TIMESTAMP, DATE;
    }
}
