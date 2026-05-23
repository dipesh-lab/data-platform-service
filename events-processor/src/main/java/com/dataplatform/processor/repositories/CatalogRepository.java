package com.dataplatform.processor.repositories;

import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.models.CatalogSchema;
import org.apache.iceberg.Table;

import java.util.List;
import java.util.Optional;

public interface CatalogRepository {

    Optional<Table> findTable(String namespace, String tableName, String bucketName) throws CatalogSchemaException;

    List<CatalogSchema> listAllSchemas();
}