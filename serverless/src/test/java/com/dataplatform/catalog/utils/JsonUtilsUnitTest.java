package com.dataplatform.catalog.utils;

import com.dataplatform.catalog.models.CatalogSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonUtilsUnitTest {

    @Test
    void deserialize_readsJson() {
        String json = """
                {
                  "namespace":"global",
                  "tableName":"api_events",
                  "bucketName":"global-api-events",
                  "partitionType":"DAY",
                  "fields":[]
                }
                """;
        var stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        CatalogSchema schema = JsonUtils.deserialize(stream, CatalogSchema.class);

        assertThat(schema.namespace()).isEqualTo("global");
        assertThat(schema.tableName()).isEqualTo("api_events");
        assertThat(schema.fields()).isEqualTo(List.of());
    }

    @Test
    void deserialize_invalidJson_throwsRuntimeException() {
        var stream = new ByteArrayInputStream("{".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> JsonUtils.deserialize(stream, String.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void getMapper_returnsSharedInstance() {
        assertThat(JsonUtils.getMapper()).isSameAs(JsonUtils.getMapper());
    }

    @Test
    void privateConstructor_canBeInvoked() throws Exception {
        Constructor<JsonUtils> constructor = JsonUtils.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        assertThat(constructor.newInstance()).isNotNull();
    }
}
