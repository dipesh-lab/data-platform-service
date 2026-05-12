package com.dataplatform.events.producer.clients;

import com.dataplatform.events.producer.models.UserEvent;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import reactor.core.publisher.Mono;

@KafkaClient
public interface DPEventClient {

    @Topic("dp-app-events")
    Mono<UserEvent> generateAppEvent(UserEvent event);

    @Topic("dp-app-events")
    Mono<UserEvent> generateAppEvent(@KafkaKey String key, UserEvent event);

}