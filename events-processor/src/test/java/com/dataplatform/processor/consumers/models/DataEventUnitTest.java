package com.dataplatform.processor.consumers.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DataEventUnitTest {

    @Test
    void record_holdsValues() {
        Instant eventTime = Instant.parse("2026-06-04T12:00:00Z");
        DataEvent event = new DataEvent("global", "api_events", "ACCOUNT-1", eventTime, Map.of("user_id", "USER-1"));

        assertThat(event.namespace()).isEqualTo("global");
        assertThat(event.type()).isEqualTo("api_events");
        assertThat(event.tenantId()).isEqualTo("ACCOUNT-1");
        assertThat(event.eventTime()).isEqualTo(eventTime);
        assertThat(event.attributes()).containsEntry("user_id", "USER-1");
    }
}
