package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.RawDataEvent;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.impl.IngestRawDataServiceImpl;
import com.github.f4b6a3.ulid.UlidCreator;
import io.micronaut.context.annotation.Property;
import io.micronaut.configuration.kafka.annotation.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.apache.iceberg.types.Types.NestedField.required;

@KafkaListener(offsetReset = OffsetReset.EARLIEST,
        properties = { @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                value = "com.dataplatform.events.processor.consumers.tranformers.RawDataEventListDeserializer"),
                @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                value = "org.apache.kafka.common.serialization.StringDeserializer") },
        errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR,
                exceptionTypes = { RetryException.class }, retryCount = 1, retryDelay = "2s"))
public class IngestRawDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(IngestRawDataConsumer.class);

    private final Schema schema;

    private final IngestRawDataServiceImpl ingestRawDataServiceImpl;
    private final KafkaClientDelegate kafkaClientDelegate;

    public IngestRawDataConsumer(IngestRawDataServiceImpl ingestRawDataServiceImpl,
                                 KafkaClientDelegate kafkaClientDelegate) {
        this.ingestRawDataServiceImpl = ingestRawDataServiceImpl;
        this.kafkaClientDelegate = kafkaClientDelegate;
        schema = createSchema();
    }

    @Topic("dp-raw-grouped-events")
    public void receive(ConsumerRecord<String, List<RawDataEvent>> record) {
        if (CollectionUtils.isNotEmpty(record.value())) {
            var events = record.value();
            var namespace = events.getFirst().namespace();
            var eventType = events.getFirst().type();
            log.info("Received total {} grouped raw events for type {} at {}", events.size(), eventType, record.timestamp());
            events.forEach(event -> log.info("Type {}, tenant {}, time {}, attrs {}",
                    event.type(), event.tenantId(), event.eventTime(), event.attributes()));

            var records = toGenericRecords(events);
            var storedRawData = ingestRawDataServiceImpl.prepare(namespace, eventType, records);
            Optional.ofNullable(storedRawData).ifPresent(kafkaClientDelegate::send);
        }
    }

    private List<GenericRecord> toGenericRecords(List<RawDataEvent> events) {
        return events.stream().filter(event -> Objects.nonNull(event.tenantId()))
                .map(event -> {
                    var record = GenericRecord.create(schema);
                    var attrs = Objects.requireNonNullElse(event.attributes(), Map.of());
                    var eventInstant = Objects.requireNonNullElse(event.eventTime(), Instant.now());
                    var eventTime = LocalDateTime.ofInstant(eventInstant, ZoneOffset.UTC);
                    record.setField("event_id", UlidCreator.getHashUlid(eventInstant.toEpochMilli(), UUID.randomUUID().toString()));
                    record.setField("tenant_id", StringUtils.defaultString(event.tenantId()));
                    record.setField("load_time", Instant.now().toEpochMilli());
                    record.setField("event_time", eventTime);
                    record.setField("user_id", attrs.getOrDefault("userId", ""));
                    record.setField("op_type", attrs.getOrDefault("opType", ""));
                    record.setField("result", attrs.getOrDefault("result", ""));
                    record.setField("error", attrs.getOrDefault("error", ""));
                    return record;
        }).toList();
    }

    private Schema createSchema() {
        return new Schema(
                required(1, "event_id", Types.StringType.get()),
                required(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.LongType.get()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                required(5, "user_id", Types.StringType.get()),
                required(6, "op_type", Types.StringType.get()),
                required(7, "result", Types.StringType.get()),
                required(8, "error", Types.StringType.get())
        );
    }
}