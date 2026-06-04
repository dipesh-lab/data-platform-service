package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.DataEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferDataEventsProcessorUnitTest {

    private static final String STORE = "buffer-store";

    @Mock
    private ProcessorContext<String, List<DataEvent>> context;
    @Mock
    private KeyValueStore<String, List<DataEvent>> store;

    private BufferDataEventsProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new BufferDataEventsProcessor(STORE, 2, "PT1M");
        when(context.getStateStore(STORE)).thenReturn(store);
        processor.init(context);
    }

    @Test
    void process_ignoresInvalidEvents() {
        processor.process(new Record<>("k", null, 0L));
        processor.process(new Record<>("k", new DataEvent("global", "", "t1", Instant.now(), Map.of()), 0L));

        verify(store, never()).put(any(), any());
        verify(context, never()).forward(any());
    }

    @Test
    void process_forwardsWhenBufferLimitReached() {
        DataEvent event = new DataEvent("global", "api_events", "t1", Instant.now(), Map.of());
        when(store.get("global|api_events")).thenReturn(null, new ArrayList<>(List.of(event)));

        processor.process(new Record<>("k", event, 0L));
        verify(store).put(eq("global|api_events"), any());
        verify(context, never()).forward(any());

        processor.process(new Record<>("k", event, 1L));
        verify(store).delete("global|api_events");
        verify(context).forward(any());
    }

    @Test
    void punctuator_forwardsBufferedEvents() {
        List<DataEvent> buffered = List.of(new DataEvent("global", "api_events", "t1", Instant.now(), Map.of()));
        @SuppressWarnings("unchecked")
        KeyValueIterator<String, List<DataEvent>> iterator = mock(KeyValueIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("global|api_events", buffered));
        when(store.all()).thenReturn(iterator);

        ArgumentCaptor<Punctuator> punctuatorCaptor = ArgumentCaptor.forClass(Punctuator.class);
        verify(context).schedule(eq(Duration.ofMinutes(1)), eq(PunctuationType.WALL_CLOCK_TIME), punctuatorCaptor.capture());
        punctuatorCaptor.getValue().punctuate(100L);

        verify(store).delete("global|api_events");
        verify(context).forward(any());
    }
}
