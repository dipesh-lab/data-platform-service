package com.dataplatform.events.processor.consumers;

import com.dataplatform.events.processor.consumers.models.StoredRawData;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface KafkaClientDelegate {

    @Topic("dp-commit-raw-data-events")
    void send(StoredRawData storeRawData);
}