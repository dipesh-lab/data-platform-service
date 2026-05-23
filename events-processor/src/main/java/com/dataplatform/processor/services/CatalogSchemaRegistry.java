package com.dataplatform.processor.services;

import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.exceptions.CatalogTableNotFoundException;
import com.dataplatform.processor.models.CatalogSchema;
import com.dataplatform.processor.repositories.CatalogRepository;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Singleton
public class CatalogSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(CatalogSchemaRegistry.class);

    private final AtomicReference<Map<String, Table>> tables = new AtomicReference<>(Map.of());
    private final AtomicReference<Map<String, String>> tableBucketNames = new AtomicReference<>(Map.of());

    private final CatalogRepository catalogRepository;

    public CatalogSchemaRegistry(CatalogRepository catalogRepository) {
        this.catalogRepository = catalogRepository;
    }

    public Table getTable(String namespace, String tableName) {
        if (StringUtils.isBlank(namespace) || StringUtils.isBlank(tableName)) {
            throw new CatalogSchemaException("Table or namespace has incorrect value");
        }
        final var key = namespace + "|" + tableName;
        if (!tableBucketNames.get().containsKey(namespace + "|" + tableName)) {
            throw new CatalogTableNotFoundException("Table [" + tableName + "] bucket not found for [" + namespace + "] namespace");
        }
        var table = tables.get().get(key);
        return Optional.ofNullable(table)
                .orElseGet(() -> {
                    synchronized (key) {
                        return catalogRepository.findTable(namespace, tableName, tableBucketNames.get().get(key))
                                .map(t -> tables.get().put(key, t))
                                .orElseThrow(() ->
                                        new CatalogTableNotFoundException("Table [" + tableName + "] not found in [" + namespace + "] namespace"));
                    }
                });
    }

    public void loadTableBucketNames(List<CatalogSchema> schemas) {
        var map = schemas.stream().collect(Collectors.toMap(
                obj -> obj.namespace() + "|" + obj.tableName(), CatalogSchema::bucketName));
        tableBucketNames.get().putAll(map);
        log.info("Total {} table to bucket names loaded", map.size());
    }

}
