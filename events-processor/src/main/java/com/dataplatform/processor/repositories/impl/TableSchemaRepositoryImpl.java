package com.dataplatform.processor.repositories.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.models.CatalogSchema;
import com.dataplatform.processor.repositories.TableSchemaRepository;
import com.dataplatform.processor.utils.JsonUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

@Singleton
public class TableSchemaRepositoryImpl implements TableSchemaRepository {

    private static final Logger log = LoggerFactory.getLogger(TableSchemaRepositoryImpl.class);

    private final PlatformDataConfig dataConfig;

    @Inject
    public TableSchemaRepositoryImpl(PlatformDataConfig dataConfig) {
        this.dataConfig = dataConfig;
    }

    @Override
    public Optional<String> findBucketName(String namespace, String tableName) {
        var schema = readTableSchema(namespace, tableName);
        return Optional.of(schema.bucketName());
    }

    public CatalogSchema readTableSchema(String namespace, String tableName) {
        log.info("Read schema for {} table and {} namespace at {} catalog directory",
                namespace, tableName, dataConfig.getCatalogDir());
        var filePath = dataConfig.getCatalogDir() + File.separator + namespace + File.separator + tableName + ".json";
        try (var stream = new FileInputStream(filePath)) {
            return JsonUtils.deserialize(stream, CatalogSchema.class);
        } catch (IOException e) {
            log.error("Error while reading table metadata", e);
            throw new CatalogSchemaException(e.getMessage(), e);
        }
    }
}
