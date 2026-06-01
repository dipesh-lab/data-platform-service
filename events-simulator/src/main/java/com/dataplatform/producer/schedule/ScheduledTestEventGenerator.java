package com.dataplatform.producer.schedule;

import com.dataplatform.producer.service.EventGeneratorService;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Requires(property = "simulator.auto-generate.enabled", value = "true", defaultValue = "false")
public class ScheduledTestEventGenerator {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTestEventGenerator.class);

    private final EventGeneratorService eventGeneratorService;
    private final int eventsPerMinute;

    public ScheduledTestEventGenerator(
            EventGeneratorService eventGeneratorService,
            @Value("${simulator.auto-generate.events-per-minute:1000}") int eventsPerMinute) {
        this.eventGeneratorService = eventGeneratorService;
        this.eventsPerMinute = eventsPerMinute;
    }

    @Scheduled(fixedRate = "1m", initialDelay = "10s")
    void generateMinuteBatch() {
        log.debug("Auto-generating {} test events", eventsPerMinute);
        eventGeneratorService.generateEvents(eventsPerMinute);
        eventGeneratorService.logBatchGenerated(eventsPerMinute);
    }
}
