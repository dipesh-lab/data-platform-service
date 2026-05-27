package com.dataplatform.processor.repositories;

import com.dataplatform.processor.exceptions.CatalogSchemaException;
import org.apache.iceberg.aws.glue.GlueCatalog;

public interface CatalogRepository {

    GlueCatalog getCatalog(String namespace, String tableName) throws CatalogSchemaException;
}