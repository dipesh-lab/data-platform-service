package com.dataplatform.processor.repositories;

import java.util.Optional;

public interface TableSchemaRepository {

    Optional<String> findBucketName(String namespace, String tableName);

}