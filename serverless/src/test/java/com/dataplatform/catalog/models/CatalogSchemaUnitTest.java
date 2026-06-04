package com.dataplatform.catalog.models;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogSchemaUnitTest {

    @Test
    void record_andEnums_holdValues() {
        CatalogSchema schema = new CatalogSchema(
                "global",
                "api_events",
                "global-api-events",
                CatalogSchema.PartitionType.DAY,
                List.of(new CatalogSchema.Field(10, "user_id", CatalogSchema.DataType.STRING, false)));

        assertThat(schema.namespace()).isEqualTo("global");
        assertThat(schema.tableName()).isEqualTo("api_events");
        assertThat(schema.partitionType()).isEqualTo(CatalogSchema.PartitionType.DAY);
        assertThat(schema.fields()).hasSize(1);
        assertThat(CatalogSchema.PartitionType.HOUR).isNotNull();
        assertThat(CatalogSchema.DataType.INT).isNotNull();
    }
}
