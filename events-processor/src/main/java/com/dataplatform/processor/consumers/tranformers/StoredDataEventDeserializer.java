package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.StoredData;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class StoredDataEventDeserializer implements Deserializer<StoredData> {

    private final Deserializer<StoredData> delegate = new JsonSerde<StoredData>(StoredData.class).deserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public StoredData deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
