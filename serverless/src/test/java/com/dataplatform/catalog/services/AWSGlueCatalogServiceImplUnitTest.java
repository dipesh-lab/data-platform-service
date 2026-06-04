package com.dataplatform.catalog.services;

import com.dataplatform.catalog.config.CatalogConfig;
import com.dataplatform.catalog.exceptions.CatalogException;
import com.dataplatform.catalog.models.ApplySchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AWSGlueCatalogServiceImplUnitTest {

    @TempDir
    Path tempDir;

    @Test
    void applySchema_missingFile_throwsCatalogException() {
        var service = new AWSGlueCatalogServiceImpl(config());

        assertThatThrownBy(() -> service.applySchema(new ApplySchema("global", "missing")))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("not exist");
    }

    @Test
    void applySchema_createsNamespaceAndTable() throws Exception {
        writeSchema("global", "api_events", "DAY", """
                [{"id":10,"name":"user_id","dataType":"STRING","deprecated":false}]
                """);

        try (MockedConstruction<GlueCatalog> ignored = mockConstruction(GlueCatalog.class, (catalog, ctx) -> {
            when(catalog.namespaceExists(any())).thenReturn(false);
            Table table = mock(Table.class);
            when(table.name()).thenReturn("api_events");
            when(table.location()).thenReturn("s3://global-api-events/");
            when(catalog.tableExists(any())).thenReturn(false);
            when(catalog.createTable(any(), any(Schema.class), any())).thenReturn(table);
        })) {
            new AWSGlueCatalogServiceImpl(config()).applySchema(new ApplySchema("global", "api_events"));

            GlueCatalog catalog = ignored.constructed().getFirst();
            verify(catalog).initialize(eq("glue_catalog"), anyMap());
            verify(catalog).createNamespace(any(), anyMap());
            verify(catalog).createTable(any(), any(Schema.class), any());
        }
    }

    @Test
    void applySchema_existingNamespaceAndHourPartition_createsTable() throws Exception {
        writeSchema("global", "hourly_events", "HOUR", """
                [{"id":10,"name":"user_id","dataType":"INT","deprecated":false}]
                """);

        try (MockedConstruction<GlueCatalog> ignored = mockConstruction(GlueCatalog.class, (catalog, ctx) -> {
            when(catalog.namespaceExists(any())).thenReturn(true);
            Table table = mock(Table.class);
            when(table.name()).thenReturn("hourly_events");
            when(table.location()).thenReturn("s3://global-api-events/");
            when(catalog.tableExists(any())).thenReturn(false);
            when(catalog.createTable(any(), any(Schema.class), any())).thenReturn(table);
        })) {
            new AWSGlueCatalogServiceImpl(config()).applySchema(new ApplySchema("global", "hourly_events"));

            GlueCatalog catalog = ignored.constructed().getFirst();
            verify(catalog).createTable(any(), any(Schema.class), any());
        }
    }

    @Test
    void applySchema_existingTable_updatesSchema() throws Exception {
        writeSchema("global", "api_events", "DAY", """
                [
                  {"id":10,"name":"user_id","dataType":"STRING","deprecated":false},
                  {"id":11,"name":"op_type","dataType":"LONG","deprecated":false},
                  {"id":12,"name":"result","dataType":"DATE","deprecated":false},
                  {"id":13,"name":"seen_at","dataType":"TIMESTAMP","deprecated":false}
                ]
                """);

        Schema existingSchema = new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get()),
                optional(99, "legacy_col", Types.StringType.get()));

        UpdateSchema updateSchema = mock(UpdateSchema.class);
        try (MockedConstruction<GlueCatalog> ignored = mockConstruction(GlueCatalog.class, (catalog, ctx) -> {
            when(catalog.namespaceExists(any())).thenReturn(true);
            when(catalog.tableExists(any())).thenReturn(true);
            Table table = mock(Table.class);
            when(table.schema()).thenReturn(existingSchema);
            when(table.updateSchema()).thenReturn(updateSchema);
            when(updateSchema.addColumn(any(), any())).thenReturn(updateSchema);
            when(updateSchema.makeColumnOptional(any())).thenReturn(updateSchema);
            when(updateSchema.deleteColumn(any())).thenReturn(updateSchema);
            when(catalog.loadTable(any())).thenReturn(table);
        })) {
            new AWSGlueCatalogServiceImpl(config()).applySchema(new ApplySchema("global", "api_events"));

            verify(updateSchema).addColumn(eq("op_type"), any());
            verify(updateSchema).addColumn(eq("result"), any());
            verify(updateSchema).addColumn(eq("seen_at"), any());
            verify(updateSchema).deleteColumn("legacy_col");
            verify(updateSchema).commit();
        }
    }

    @Test
    void applySchema_existingTable_noChanges_onlyCommits() throws Exception {
        writeSchema("global", "api_events", "DAY", """
                [{"id":10,"name":"user_id","dataType":"STRING","deprecated":false}]
                """);

        Schema existingSchema = new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get()));

        UpdateSchema updateSchema = mock(UpdateSchema.class);
        try (MockedConstruction<GlueCatalog> ignored = mockConstruction(GlueCatalog.class, (catalog, ctx) -> {
            when(catalog.namespaceExists(any())).thenReturn(true);
            when(catalog.tableExists(any())).thenReturn(true);
            Table table = mock(Table.class);
            when(table.schema()).thenReturn(existingSchema);
            when(table.updateSchema()).thenReturn(updateSchema);
            when(catalog.loadTable(any())).thenReturn(table);
        })) {
            new AWSGlueCatalogServiceImpl(config()).applySchema(new ApplySchema("global", "api_events"));

            verify(updateSchema, never()).addColumn(any(), any());
            verify(updateSchema, never()).deleteColumn(any());
            verify(updateSchema).commit();
        }
    }

    private CatalogConfig config() {
        return new CatalogConfig("ap-southeast-2", tempDir.toString(), "arn:aws:iam::1:role/glue");
    }

    private void writeSchema(String namespace, String tableName, String partitionType, String fieldsJson)
            throws IOException {
        Path namespaceDir = tempDir.resolve(namespace);
        Files.createDirectories(namespaceDir);
        String json = """
                {
                  "namespace":"%s",
                  "tableName":"%s",
                  "bucketName":"global-api-events",
                  "partitionType":"%s",
                  "fields":%s
                }
                """.formatted(namespace, tableName, partitionType, fieldsJson);
        Files.writeString(namespaceDir.resolve(tableName + ".json"), json);
    }
}
