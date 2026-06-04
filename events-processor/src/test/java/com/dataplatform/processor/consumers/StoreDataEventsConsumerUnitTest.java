package com.dataplatform.processor.consumers;

import com.dataplatform.processor.catalog.CachedCatalogTableRegistry;
import com.dataplatform.processor.clients.KafkaClientDelegate;
import com.dataplatform.processor.consumers.models.DataEvent;
import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.services.IngestDataService;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoreDataEventsConsumerUnitTest {

    @Mock
    private IngestDataService ingestDataService;
    @Mock
    private KafkaClientDelegate kafkaClientDelegate;
    @Mock
    private CachedCatalogTableRegistry catalogTableRegistry;
    @Mock
    private Table table;

    @InjectMocks
    private StoreDataEventsConsumer consumer;

    @Test
    void receive_emptyBatch_doesNothing() throws Exception {
        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", List.of()));

        verifyNoInteractions(ingestDataService, kafkaClientDelegate);
    }

    @Test
    void receive_storesDataAndPublishesStagedFile() throws Exception {
        DataEvent event = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.parse("2026-06-04T12:00:00Z"),
                Map.of("user_id", "USER-1", "count", 10, "amount", 20L, "active", true,
                        "day", "2026-06-04", "seen_at", "2026-06-04T12:00:00"));
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/data/file.parquet", 1, 512L, null);
        Schema schema = new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get()),
                optional(11, "count", Types.IntegerType.get()),
                optional(12, "amount", Types.LongType.get()),
                optional(13, "active", Types.BooleanType.get()),
                optional(14, "day", Types.DateType.get()),
                optional(15, "seen_at", Types.TimestampType.withoutZone()));

        when(catalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.schema()).thenReturn(schema);
        when(ingestDataService.storeData(eq("global"), eq("api_events"), any())).thenReturn(staged);

        consumer.receive(new ConsumerRecord<>("global|api_events", 0, 0, "global|api_events", List.of(event)));

        verify(ingestDataService).storeData(eq("global"), eq("api_events"), any());
        verify(kafkaClientDelegate).send("global|api_events", staged);
    }

    @Test
    void receive_storeFailure_isHandledGracefully() throws Exception {
        DataEvent event = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.now(), Map.of("user_id", "U1"));
        when(catalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.schema()).thenReturn(new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get())));
        when(ingestDataService.storeData(any(), any(), any())).thenThrow(new RuntimeException("store failed"));

        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", List.of(event)));

        verifyNoInteractions(kafkaClientDelegate);
    }

    @Test
    void receive_nullTenantId_skipsPublishing() throws Exception {
        DataEvent event = new DataEvent("global", "api_events", null, Instant.now(), Map.of());
        when(catalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.schema()).thenReturn(new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone())));

        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", List.of(event)));

        verify(ingestDataService).storeData(eq("global"), eq("api_events"), any());
        verifyNoInteractions(kafkaClientDelegate);
    }

    @Test
    void receive_nullStagedResult_doesNotPublish() throws Exception {
        DataEvent event = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.now(), Map.of("user_id", "U1"));
        when(catalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.schema()).thenReturn(new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get())));
        when(ingestDataService.storeData(any(), any(), any())).thenReturn(null);

        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", List.of(event)));

        verify(ingestDataService).storeData(eq("global"), eq("api_events"), any());
        verifyNoInteractions(kafkaClientDelegate);
    }

    @Test
    void receive_mapsNumericAndTimestampAttributes() throws Exception {
        DataEvent event = new DataEvent("global", "api_events", "ACCOUNT-1", Instant.parse("2026-06-04T12:00:00Z"),
                Map.of("user_id", "USER-1", "ratio", 1.5f, "score", 9.5d, "seen_at", LocalDateTime.parse("2026-06-04T12:00:00")));
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/data/file.parquet", 1, 512L, null);
        Schema schema = new Schema(
                required(1, "event_id", Types.StringType.get()),
                optional(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.TimestampType.withoutZone()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                optional(10, "user_id", Types.StringType.get()),
                optional(11, "ratio", Types.FloatType.get()),
                optional(12, "score", Types.DoubleType.get()),
                optional(13, "seen_at", Types.TimestampType.withoutZone()));

        when(catalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.schema()).thenReturn(schema);
        when(ingestDataService.storeData(eq("global"), eq("api_events"), any())).thenReturn(staged);

        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", List.of(event)));

        verify(ingestDataService).storeData(eq("global"), eq("api_events"), any());
        verify(kafkaClientDelegate).send("global|api_events", staged);
    }
}
