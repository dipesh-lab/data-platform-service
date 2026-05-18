package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredRawData;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.impl.IngestRawDataServiceImpl;
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

    private final IngestRawDataServiceImpl ingestRawDataServiceImpl;

    public CommitRawDataConsumer(IngestRawDataServiceImpl ingestRawDataServiceImpl) {
        this.ingestRawDataServiceImpl = ingestRawDataServiceImpl;
    }

    @Topic("dp-commit-raw-data-events")
    public void receive(ConsumerRecord<String, StoredRawData> record) {
        Optional.ofNullable(record.value()).ifPresent(ingestRawDataServiceImpl::writeData);
    }
}
