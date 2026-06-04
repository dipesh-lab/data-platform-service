package com.dataplatform.producer.rest.resources;

import com.dataplatform.producer.service.EventGeneratorService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DataResourceUnitTest {

    @Mock
    private EventGeneratorService eventGeneratorService;

    @InjectMocks
    private DataResource dataResource;

    @Test
    void generateEvents_returnsAcceptedAndDelegatesToService() {
        HttpResponse<String> response = dataResource.generateEvents(10);

        assertThat(response.status().getCode()).isEqualTo(HttpStatus.ACCEPTED.getCode());
        assertThat(response.body()).contains("10 events are created");
        verify(eventGeneratorService).generateEvents(10);
        verify(eventGeneratorService).logBatchGenerated(10);
    }

    @Test
    void generateEvents_usesDefaultCountFromCaller() {
        HttpResponse<String> response = dataResource.generateEvents(5);

        assertThat(response.body()).contains("5 events are created");
        verify(eventGeneratorService).generateEvents(5);
        verify(eventGeneratorService).logBatchGenerated(5);
    }
}
