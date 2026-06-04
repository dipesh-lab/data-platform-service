package com.dataplatform.producer.service;

import com.dataplatform.producer.TestEventData;
import com.dataplatform.producer.clients.DPEventClient;
import com.dataplatform.producer.models.DataEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventGeneratorServiceUnitTest {

    @Mock
    private DPEventClient eventClient;

    @InjectMocks
    private EventGeneratorService eventGeneratorService;

    @Test
    void generateEvents_publishesRequestedCount() {
        when(eventClient.generateAppEvent(any(DataEvent.class))).thenReturn(Mono.empty());

        eventGeneratorService.generateEvents(3);

        verify(eventClient, times(3)).generateAppEvent(any(DataEvent.class));
    }

    @Test
    void publishRandomEvent_usesTestDataPools() {
        when(eventClient.generateAppEvent(any(DataEvent.class))).thenReturn(Mono.empty());
        ArgumentCaptor<DataEvent> captor = ArgumentCaptor.forClass(DataEvent.class);

        eventGeneratorService.publishRandomEvent();

        verify(eventClient).generateAppEvent(captor.capture());
        DataEvent event = captor.getValue();
        assertThat(event.namespace()).isEqualTo("global");
        assertThat(event.type()).isEqualTo("api_events");
        assertThat(TestEventData.ACCOUNT_IDS).contains(event.tenantId());
        assertThat(TestEventData.USER_IDS).contains(String.valueOf(event.attributes().get("user_id")));
        assertThat(TestEventData.OP_TYPES).contains(String.valueOf(event.attributes().get("op_type")));
        assertThat(event.attributes().get("result")).isEqualTo("SUCCESS");
    }

    @Test
    void generateEvents_zeroCount_doesNotPublish() {
        eventGeneratorService.generateEvents(0);

        verifyNoInteractions(eventClient);
    }

    @Test
    void publishRandomEvent_setsEventTime() {
        when(eventClient.generateAppEvent(any(DataEvent.class))).thenReturn(Mono.empty());
        ArgumentCaptor<DataEvent> captor = ArgumentCaptor.forClass(DataEvent.class);

        eventGeneratorService.publishRandomEvent();

        verify(eventClient).generateAppEvent(captor.capture());
        assertThat(captor.getValue().eventTime()).isNotNull();
    }

    @Test
    void logBatchGenerated_runsWithoutError() {
        eventGeneratorService.logBatchGenerated(5);
    }
}
