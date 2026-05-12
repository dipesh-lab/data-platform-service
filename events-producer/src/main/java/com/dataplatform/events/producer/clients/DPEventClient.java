package com.dataplatform.events.producer.clients;

import com.dataplatform.events.producer.models.DataEvent;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import reactor.core.publisher.Mono;

@KafkaClient
public interface DPEventClient {

    @Topic("dp-raw-events")
    Mono<DataEvent> generateAppEvent(DataEvent event);

    @Topic("dp-raw-events")
    Mono<DataEvent> generateAppEvent(@KafkaKey String key, DataEvent event);

}