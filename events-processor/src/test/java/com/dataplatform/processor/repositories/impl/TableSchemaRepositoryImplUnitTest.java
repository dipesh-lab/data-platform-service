package com.dataplatform.processor.repositories.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.models.CatalogSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableSchemaRepositoryImplUnitTest {

    @TempDir
    Path tempDir;

    private TableSchemaRepositoryImpl repository;

    @BeforeEach
    void setUp() {
        PlatformDataConfig config = new PlatformDataConfig();
        config.setCatalogDir(tempDir.toString());
        repository = new TableSchemaRepositoryImpl(config);
    }

    @Test
    void findBucketName_readsFromSchemaFile() throws Exception {
        writeSchema("global", "api_events", "global-api-events");

        assertThat(repository.findBucketName("global", "api_events"))
                .contains("global-api-events");
    }

    @Test
    void readTableSchema_parsesCatalogSchema() throws Exception {
        writeSchema("global", "api_events", "global-api-events");

        CatalogSchema schema = repository.readTableSchema("global", "api_events");

        assertThat(schema.namespace()).isEqualTo("global");
        assertThat(schema.tableName()).isEqualTo("api_events");
        assertThat(schema.bucketName()).isEqualTo("global-api-events");
    }

    @Test
    void readTableSchema_missingFile_throwsCatalogSchemaException() {
        assertThatThrownBy(() -> repository.readTableSchema("global", "missing"))
                .isInstanceOf(CatalogSchemaException.class);
    }

    private void writeSchema(String namespace, String tableName, String bucketName) throws Exception {
        Path namespaceDir = tempDir.resolve(namespace);
        Files.createDirectories(namespaceDir);
        String json = """
                {
                  "namespace":"%s",
                  "tableName":"%s",
                  "bucketName":"%s",
                  "partitionType":"DAY",
                  "fields":[]
                }
                """.formatted(namespace, tableName, bucketName);
        Files.writeString(namespaceDir.resolve(tableName + ".json"), json);
    }
}
