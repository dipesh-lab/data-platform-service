package com.dataplatform.processor.consumers;

import com.dataplatform.processor.catalog.CachedCatalogTableRegistry;
import com.dataplatform.processor.consumers.models.RawDataEvent;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.IngestDataService;
import com.github.f4b6a3.ulid.UlidCreator;
import io.micronaut.context.annotation.Property;
import io.micronaut.configuration.kafka.annotation.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.types.Type;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ExecutionException;

@KafkaListener(offsetReset = OffsetReset.EARLIEST,
        properties = { @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                value = "com.dataplatform.processor.consumers.tranformers.RawDataEventListDeserializer"),
                @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                value = "org.apache.kafka.common.serialization.StringDeserializer") },
        errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR,
                exceptionTypes = { RetryException.class }, retryCount = 1, retryDelay = "2s"))
public class IngestRawDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(IngestRawDataConsumer.class);

    private final Set<String> commonFields = Set.of("event_id", "tenant_id", "event_time", "load_time");

    private final IngestDataService ingestRawDataService;
    private final KafkaClientDelegate kafkaClientDelegate;
    private final CachedCatalogTableRegistry catalogTableRegistry;

    public IngestRawDataConsumer(IngestDataService ingestRawDataService,
                                 KafkaClientDelegate kafkaClientDelegate,
                                 CachedCatalogTableRegistry catalogTableRegistry) {
        this.ingestRawDataService = ingestRawDataService;
        this.kafkaClientDelegate = kafkaClientDelegate;
        this.catalogTableRegistry = catalogTableRegistry;
    }

    @Topic("dp-raw-grouped-events")
    public void receive(ConsumerRecord<String, List<RawDataEvent>> record) throws Exception {
        if (CollectionUtils.isNotEmpty(record.value())) {
            try {
                var events = record.value();
                var namespace = events.getFirst().namespace();
                var eventType = events.getFirst().type();
                log.info("Received total {} grouped events for type {}, namespace {}", events.size(), eventType, namespace);

                var records = toGenericRecords(namespace, eventType, events);
                var storedRawData = ingestRawDataService.prepare(namespace, eventType, records);
                Optional.ofNullable(storedRawData).ifPresent(kafkaClientDelegate::send);
            } catch (Exception e) {
                log.error("An error occurred. {}", e.getMessage(), e);
            } catch (Throwable t) {
                log.error("Fatal error in ingest consumer [{}]: {}", t.getClass().getName(), t.getMessage(), t);
            }
        }
    }

    private List<GenericRecord> toGenericRecords(String namespace, String tableName, List<RawDataEvent> events) throws ExecutionException {
        var table = catalogTableRegistry.getTable(namespace, tableName);
        var schema = table.schema();
        var fields = schema.columns().stream().filter(field -> !commonFields.contains(field.name())).toList();
        return events.stream().filter(event -> Objects.nonNull(event.tenantId()))
                .map(event -> {
                    var record = GenericRecord.create(schema);
                    var attrs = Objects.requireNonNullElse(event.attributes(), Map.<String, Object>of());
                    var eventInstant = Objects.requireNonNullElse(event.eventTime(), Instant.now());
                    var eventTime = LocalDateTime.ofInstant(eventInstant, ZoneOffset.UTC);

                    record.setField("event_id", UlidCreator.getHashUlid(eventInstant.toEpochMilli(), UUID.randomUUID().toString()).toString());
                    record.setField("tenant_id", StringUtils.defaultString(event.tenantId()));
                    record.setField("load_time", LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
                    record.setField("event_time", eventTime);

                    fields.forEach(field -> record.setField(field.name(), getFieldValue(field.name(), field.type(), attrs)));
                    return record;
        }).toList();
    }

    private Object getFieldValue(String fieldName, Type type, Map<String, Object> attrs) {
        return Optional.ofNullable(attrs.get(fieldName))
                .map(value -> getValue(value, type))
                .orElse(null);
    }

    private static Object getValue(Object value, Type type) {
        return switch (type.typeId()) {
            case STRING -> String.valueOf(value);
            case INTEGER -> value instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(value));
            case LONG -> value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
            case FLOAT -> value instanceof Number n ? n.floatValue() : Float.parseFloat(String.valueOf(value));
            case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
            case BOOLEAN -> value instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(value));
            case DATE -> value instanceof LocalDate ld ? ld : LocalDate.parse(String.valueOf(value));
            case TIMESTAMP -> toTimestamp(value, type);
            default -> value;
        };
    }

    private static Object toTimestamp(Object value, Type type) {
        if (value instanceof LocalDateTime) {
            return value;
        }
        if (value instanceof Instant instant) {
            return type instanceof Types.TimestampType ts && ts.shouldAdjustToUTC()
                    ? instant
                    : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return LocalDateTime.parse(String.valueOf(value));
    }
}