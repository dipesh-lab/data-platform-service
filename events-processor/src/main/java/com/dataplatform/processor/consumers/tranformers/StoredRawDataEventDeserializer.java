package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.consumers.models.StoredRawData;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class StoredRawDataEventDeserializer implements Deserializer<StoredRawData> {

    private final Deserializer<StoredRawData> delegate = new JsonSerde<StoredRawData>(StoredRawData.class).deserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public StoredRawData deserialize(String topic, byte[] data) {
        return delegate.deserialize(topic, data);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
