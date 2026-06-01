package com.dataplatform.producer.rest.resources;

import com.dataplatform.producer.service.EventGeneratorService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller("/events/generate")
public class DataResource {

    private static final Logger log = LoggerFactory.getLogger(DataResource.class);

    private final EventGeneratorService eventGeneratorService;

    public DataResource(EventGeneratorService eventGeneratorService) {
        this.eventGeneratorService = eventGeneratorService;
    }

    @Get
    public HttpResponse<String> generateEvents(@QueryValue(value = "count", defaultValue = "5") Integer eventCount) {
        log.info("Request received to generate {} events", eventCount);
        eventGeneratorService.generateEvents(eventCount);
        eventGeneratorService.logBatchGenerated(eventCount);
        return HttpResponse.accepted().body(eventCount + " events are created");
    }
}
