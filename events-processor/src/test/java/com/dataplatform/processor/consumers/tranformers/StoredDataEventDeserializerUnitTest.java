package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.StoredData;
import org.junit.jupiter.api.Test;

import java.util.List;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StoredDataEventDeserializerUnitTest {

    private final StoredDataEventDeserializer deserializer = new StoredDataEventDeserializer();

    @Test
    void deserialize_readsStoredData() {
        var original = new StoredData("global", "api_events", "s3://bucket/file.parquet", 2, 64L,
                List.of("s3://bucket/a.parquet"));
        byte[] bytes = new JsonSerde<>(StoredData.class).serializer().serialize("topic", original);

        StoredData restored = deserializer.deserialize("topic", bytes);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void configureAndClose_delegateLifecycle() {
        deserializer.configure(Map.of(), false);
        deserializer.close();
    }
}
