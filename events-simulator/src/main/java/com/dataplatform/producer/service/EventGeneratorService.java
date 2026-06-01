package com.dataplatform.producer.service;

import com.dataplatform.producer.TestEventData;
import com.dataplatform.producer.clients.DPEventClient;
import com.dataplatform.producer.models.DataEvent;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Random;

@Singleton
public class EventGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(EventGeneratorService.class);

    private final DPEventClient eventClient;
    private final Random random = new Random();

    public EventGeneratorService(DPEventClient eventClient) {
        this.eventClient = eventClient;
    }

    /**
     * Publishes {@code count} random events using {@link TestEventData} pools.
     */
    public void generateEvents(int count) {
        for (int i = 0; i < count; i++) {
            publishRandomEvent();
        }
    }

    public void publishRandomEvent() {
        var accountId = randomFrom(TestEventData.ACCOUNT_IDS);
        var userId = randomFrom(TestEventData.USER_IDS);
        var opType = randomFrom(TestEventData.OP_TYPES);
        var event = DataEvent.builder()
                .namespace("global")
                .tenantId(accountId)
                .type("api_events")
                .eventTime(Instant.now())
                .attributes(Map.of(
                        "user_id", userId,
                        "op_type", opType,
                        "result", "SUCCESS"))
                .build();
        eventClient.generateAppEvent(event).subscribe();
    }

    public void logBatchGenerated(int count) {
        log.info("Generated {} test events", count);
    }

    private String randomFrom(java.util.List<String> values) {
        return values.get(random.nextInt(values.size()));
    }
}
