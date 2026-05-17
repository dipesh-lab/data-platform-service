package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredRawData;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface KafkaClientDelegate {

    @Topic("dp-commit-raw-data-events")
    void send(StoredRawData storeRawData);
}