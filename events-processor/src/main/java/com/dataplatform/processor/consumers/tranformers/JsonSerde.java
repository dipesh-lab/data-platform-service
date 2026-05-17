package com.dataplatform.processor.consumers.tranformers;

import com.dataplatform.processor.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class JsonSerde<T> implements Serde<T> {

    private final Class<T> clazz;

    public JsonSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return JsonUtils.getMapper().writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            try {
                return JsonUtils.getMapper().readValue(data, clazz);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}