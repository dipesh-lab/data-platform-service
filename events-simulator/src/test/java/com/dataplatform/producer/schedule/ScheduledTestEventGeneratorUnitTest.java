package com.dataplatform.producer.schedule;

import com.dataplatform.producer.service.EventGeneratorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ScheduledTestEventGeneratorUnitTest {

    @Mock
    private EventGeneratorService eventGeneratorService;

    @Test
    void generateMinuteBatch_usesConfiguredRate() {
        var generator = new ScheduledTestEventGenerator(eventGeneratorService, 250);

        generator.generateMinuteBatch();

        verify(eventGeneratorService).generateEvents(250);
        verify(eventGeneratorService).logBatchGenerated(250);
    }

    @Test
    void generateMinuteBatch_supportsLargeBatchSize() {
        var generator = new ScheduledTestEventGenerator(eventGeneratorService, 1000);

        generator.generateMinuteBatch();

        verify(eventGeneratorService).generateEvents(1000);
        verify(eventGeneratorService).logBatchGenerated(1000);
    }
}
