package com.dataplatform.events.producer.models;

import io.micronaut.serde.annotation.Serdeable;
import lombok.Builder;
import lombok.Getter;

@Serdeable
@Builder
@Getter
public class UserEvent {

    private final String eventId; // could be request-id
    private final String accountId;
    private final String userId;
    private final String type;
    private final String result;
    private final String error;
    private final Long eventTime;
}
