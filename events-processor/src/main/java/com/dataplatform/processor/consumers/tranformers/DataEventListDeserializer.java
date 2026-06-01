package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.DataEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataEventListDeserializer implements Deserializer<List<DataEvent>> {

    private final Deserializer<List<DataEvent>> delegate =
            Serdes.ListSerde(ArrayList.class, new JsonSerde<>(DataEvent.class)).deserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public List<DataEvent> deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
