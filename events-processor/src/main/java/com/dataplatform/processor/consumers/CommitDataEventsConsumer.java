package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.IngestDataService;
import com.dataplatform.processor.utils.Constants;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Property;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

@KafkaListener(offsetReset = OffsetReset.EARLIEST,
        properties = { @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                value = "com.dataplatform.processor.consumers.tranformers.StoredDataEventDeserializer"),
                @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        value = "org.apache.kafka.common.serialization.StringDeserializer") },
        errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR,
                exceptionTypes = { RetryException.class }, retryCount = 1, retryDelay = "2s"))
public class CommitDataEventsConsumer {

    private final IngestDataService ingestDataService;

    public CommitDataEventsConsumer(IngestDataService ingestDataService) {
        this.ingestDataService = ingestDataService;
    }

    @Topic(Constants.Topic.COMMIT_DATA_EVENTS)
    public void receive(ConsumerRecord<String, StoredData> record) {
        Optional.ofNullable(record.value()).ifPresent(ingestDataService::commitStageFile);
    }
}
