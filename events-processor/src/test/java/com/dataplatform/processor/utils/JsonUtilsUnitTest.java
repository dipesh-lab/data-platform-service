package com.dataplatform.processor.utils;

import com.dataplatform.processor.models.CatalogSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class JsonUtilsUnitTest {

    @Test
    void deserialize_readsCatalogSchema() throws Exception {
        String json = """
                {
                  "namespace":"global",
                  "tableName":"api_events",
                  "bucketName":"global-api-events",
                  "partitionType":"DAY",
                  "fields":[]
                }
                """;
        CatalogSchema schema = JsonUtils.deserialize(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), CatalogSchema.class);

        assertThat(schema.namespace()).isEqualTo("global");
        assertThat(schema.bucketName()).isEqualTo("global-api-events");
    }

    @Test
    void getMapper_isShared() {
        assertThat(JsonUtils.getMapper()).isSameAs(JsonUtils.getMapper());
    }
}
