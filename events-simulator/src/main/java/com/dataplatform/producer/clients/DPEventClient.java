package com.dataplatform.producer.clients;

import com.dataplatform.producer.models.DataEvent;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.Topic;
import reactor.core.publisher.Mono;

@KafkaClient
public interface DPEventClient {

    @Topic("dp-data-events")
    Mono<DataEvent> generateAppEvent(DataEvent event);
}