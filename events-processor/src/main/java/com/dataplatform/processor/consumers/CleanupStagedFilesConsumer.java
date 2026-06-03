package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.utils.Constants;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Property;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KafkaListener(groupId = "dp-cleanup", offsetReset = OffsetReset.EARLIEST,
        properties = { @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                value = "com.dataplatform.processor.consumers.tranformers.StoredDataEventDeserializer"),
                @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        value = "org.apache.kafka.common.serialization.StringDeserializer") },
        errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_CONDITIONALLY_ON_ERROR,
                exceptionTypes = { RetryException.class }, retryCount = 1, retryDelay = "2s"))
public class CleanupStagedFilesConsumer {

    private static final Logger log = LoggerFactory.getLogger(CleanupStagedFilesConsumer.class);

    @Topic(Constants.Topic.COMMIT_DATA_EVENTS)
    public void receive(ConsumerRecord<String, StoredData> record) {
        log.info("Staged file cleanup event received");
    }
}
