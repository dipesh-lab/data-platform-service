package com.dataplatform.events.processor.consumers.tranformers;

import com.dataplatform.events.processor.consumers.models.RawDataEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RawDataEventListDeserializer implements Deserializer<List<RawDataEvent>> {

    private final Deserializer<List<RawDataEvent>> delegate =
            Serdes.ListSerde(ArrayList.class, new JsonSerde<>(RawDataEvent.class)).deserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public List<RawDataEvent> deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
