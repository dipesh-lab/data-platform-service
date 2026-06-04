package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.services.IngestDataService;
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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MergeDataEventsProcessorUnitTest {

    private static final String STORE = MergeDataEventsTopology.STORE_NAME;

    @Mock
    private ProcessorContext<String, StoredData> context;
    @Mock
    private KeyValueStore<String, List<StoredData>> store;
    @Mock
    private IngestDataService ingestDataService;

    private MergeDataEventsProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new MergeDataEventsProcessor(STORE, Duration.ofMinutes(3), ingestDataService);
        when(context.getStateStore(STORE)).thenReturn(store);
        processor.init(context);
    }

    @Test
    void process_ignoresInvalidStagedData() {
        processor.process(new Record<>("k", null, 0L));
        processor.process(new Record<>("k", new StoredData("", "api_events", "loc", 1, 1L, null), 0L));

        verify(store, never()).put(any(), any());
    }

    @Test
    void process_queuesStagedFileInStore() {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/a.parquet", 5, 100L, null);

        processor.process(new Record<>("global|api_events", staged, 0L));

        verify(store).put(eq("global|api_events"), any());
    }

    @Test
    void flushAll_mergesAndForwards() throws Exception {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/a.parquet", 5, 100L, null);
        StoredData merged = new StoredData("global", "api_events", "s3://bucket/merged.parquet", 5, 200L, List.of("s3://bucket/a.parquet"));
        when(ingestDataService.mergeStoredFiles("global", "api_events", List.of(staged))).thenReturn(merged);

        @SuppressWarnings("unchecked")
        KeyValueIterator<String, List<StoredData>> iterator = mock(KeyValueIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("global|api_events", List.of(staged)));
        when(store.all()).thenReturn(iterator);

        ArgumentCaptor<Punctuator> punctuatorCaptor = ArgumentCaptor.forClass(Punctuator.class);
        verify(context).schedule(eq(Duration.ofMinutes(3)), eq(PunctuationType.WALL_CLOCK_TIME), punctuatorCaptor.capture());
        punctuatorCaptor.getValue().punctuate(1000L);

        verify(ingestDataService).mergeStoredFiles("global", "api_events", List.of(staged));
        verify(context).forward(any());
        verify(store).delete("global|api_events");
    }

    @Test
    void flushAll_mergeFailureDoesNotForward() throws Exception {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/a.parquet", 5, 100L, null);
        when(ingestDataService.mergeStoredFiles("global", "api_events", List.of(staged)))
                .thenThrow(new RuntimeException("merge failed"));

        @SuppressWarnings("unchecked")
        KeyValueIterator<String, List<StoredData>> iterator = mock(KeyValueIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair("global|api_events", List.of(staged)));
        when(store.all()).thenReturn(iterator);

        ArgumentCaptor<Punctuator> punctuatorCaptor = ArgumentCaptor.forClass(Punctuator.class);
        verify(context).schedule(any(), eq(PunctuationType.WALL_CLOCK_TIME), punctuatorCaptor.capture());
        punctuatorCaptor.getValue().punctuate(1000L);

        verify(context, never()).forward(any());
    }
}
