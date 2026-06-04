package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.DataEvent;
import com.dataplatform.processor.consumers.models.StoredData;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSerdeUnitTest {

    private final JsonSerde<DataEvent> dataEventSerde = new JsonSerde<>(DataEvent.class);
    private final JsonSerde<StoredData> storedDataSerde = new JsonSerde<>(StoredData.class);

    @Test
    void dataEventRoundTrip() {
        var original = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.parse("2026-06-04T12:00:00Z"),
                Map.of("user_id", "USER-1"));
        byte[] bytes = dataEventSerde.serializer().serialize("dp-data-events", original);
        DataEvent restored = dataEventSerde.deserializer().deserialize("dp-data-events", bytes);
        assertThat(restored.namespace()).isEqualTo("global");
        assertThat(restored.tenantId()).isEqualTo("ACCOUNT-1");
    }

    @Test
    void storedDataRoundTrip() {
        var original = new StoredData("global", "api_events", "s3://bucket/file.parquet", 10, 100L, null);
        byte[] bytes = storedDataSerde.serializer().serialize("dp-merge-data-events", original);
        StoredData restored = storedDataSerde.deserializer().deserialize("dp-merge-data-events", bytes);
        assertThat(restored).isEqualTo(original);
    }

    @Test
    void deserializeTombstoneAsNull() {
        assertThat(storedDataSerde.deserializer().deserialize("topic", null)).isNull();
        assertThat(storedDataSerde.deserializer().deserialize("topic", new byte[0])).isNull();
    }

    @Test
    void deserializeInvalidJson_throwsRuntimeException() {
        assertThatThrownBy(() -> storedDataSerde.deserializer().deserialize("topic", "{".getBytes()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to deserialize StoredData");
    }

    @Test
    void serializeNull_writesJsonNullLiteral() {
        assertThat(new String(dataEventSerde.serializer().serialize("topic", null))).isEqualTo("null");
    }
}
