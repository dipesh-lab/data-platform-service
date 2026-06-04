package com.dataplatform.producer.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataEventUnitTest {

    @Test
    void builder_createsEvent() {
        Instant now = Instant.parse("2026-06-04T12:00:00Z");
        DataEvent event = DataEvent.builder()
                .namespace("global")
                .type("api_events")
                .tenantId("ACCOUNT-1")
                .eventTime(now)
                .attributes(Map.of("user_id", "USER-1"))
                .build();

        assertThat(event.namespace()).isEqualTo("global");
        assertThat(event.type()).isEqualTo("api_events");
        assertThat(event.tenantId()).isEqualTo("ACCOUNT-1");
        assertThat(event.eventTime()).isEqualTo(now);
        assertThat(event.attributes()).containsEntry("user_id", "USER-1");
    }
}
