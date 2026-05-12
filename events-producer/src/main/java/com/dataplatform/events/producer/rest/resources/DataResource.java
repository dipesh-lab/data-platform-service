package com.dataplatform.events.producer.rest.resources;

import com.dataplatform.events.producer.clients.DPEventClient;
import com.dataplatform.events.producer.models.DataEvent;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

@Controller("/events/generate")
public class DataResource {

    private static final Logger log = LoggerFactory.getLogger(DataResource.class);

    private final List<String> accountIds = List.of("ACCOUNT-1", "ACCOUNT-3", "ACCOUNT-5");
    private final List<String> userIds = List.of("USER-1", "USER-2", "USER-3", "USER-4", "USER-5");
    private final Random random = new Random();

    private final DPEventClient eventClient;

    public DataResource(DPEventClient eventClient) {
        this.eventClient = eventClient;
    }

    @Get
    public HttpResponse<String> generateEvents(@QueryValue(value = "count", defaultValue = "5") Integer eventCount) {
        log.info("Request received to generate {} events", eventCount);
        long time = System.currentTimeMillis();
        IntStream.iterate(0, i -> i + 1).limit(eventCount).forEach(i -> {
            var accountId = accountIds.get(random.nextInt(3));
            var userId = userIds.get(random.nextInt(5));
            var event = DataEvent.builder()
                    .namespace("IdentityPlatform")
                    .tenantId(accountId)
                    .type("app_events")
                    .eventTime(Instant.now())
                    .attributes(Map.of("user_id", userId, "op_type", "Api.GetIntegrations", "result", "SUCCESS"))
                    .build();
            eventClient.generateAppEvent(accountId, event).subscribe();
        });
        return HttpResponse.accepted().body(eventCount + " events are created");
    }

}
