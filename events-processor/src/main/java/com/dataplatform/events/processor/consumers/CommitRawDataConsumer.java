package com.dataplatform.events.processor.consumers;

import com.dataplatform.events.processor.consumers.models.StoredRawData;
import com.dataplatform.events.processor.exceptions.RetryException;
import com.dataplatform.events.processor.services.RawDataService;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Property;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

@KafkaListener(offsetReset = OffsetReset.EARLIEST,
        properties = { @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                value = "com.dataplatform.events.processor.consumers.tranformers.StoredRawDataEventDeserializer"),
                @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        value = "org.apache.kafka.common.serialization.StringDeserializer") },
        errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR,
                exceptionTypes = { RetryException.class }, retryCount = 1, retryDelay = "2s"))
public class CommitRawDataConsumer {

    private final RawDataService rawDataService;

    public CommitRawDataConsumer(RawDataService rawDataService) {
        this.rawDataService = rawDataService;
    }

    @Topic("dp-commit-raw-data-events")
    public void receive(ConsumerRecord<String, StoredRawData> record) {
        Optional.ofNullable(record.value()).ifPresent(rawDataService::writeToTable);
    }
}
