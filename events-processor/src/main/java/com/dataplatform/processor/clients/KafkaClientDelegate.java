package com.dataplatform.processor.clients;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.utils.Constants;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient
public interface KafkaClientDelegate {

    @Topic(Constants.Topic.MERGE_DATA_EVENTS)
    void send(@KafkaKey String key, StoredData storeRawData);

}