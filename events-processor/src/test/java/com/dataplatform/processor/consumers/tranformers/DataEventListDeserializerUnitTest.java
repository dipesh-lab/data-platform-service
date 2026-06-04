package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.DataEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataEventListDeserializerUnitTest {

    private final DataEventListDeserializer deserializer = new DataEventListDeserializer();

    @Test
    void deserialize_readsEventList() {
        var event = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.parse("2026-06-04T12:00:00Z"), Map.of());
        byte[] bytes = Serdes.ListSerde(ArrayList.class, new JsonSerde<>(DataEvent.class))
                .serializer()
                .serialize("topic", new ArrayList<>(List.of(event)));

        List<DataEvent> restored = deserializer.deserialize("topic", bytes);

        assertThat(restored).hasSize(1);
        assertThat(restored.getFirst().namespace()).isEqualTo("global");
    }

    @Test
    void configureAndClose_delegateLifecycle() {
        deserializer.configure(Map.of(), false);
        deserializer.close();
    }
}
