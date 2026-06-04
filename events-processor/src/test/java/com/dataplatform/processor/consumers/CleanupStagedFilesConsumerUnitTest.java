package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class CleanupStagedFilesConsumerUnitTest {

    private final CleanupStagedFilesConsumer consumer = new CleanupStagedFilesConsumer();

    @Test
    void receive_logsCleanupEvent() {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/file.parquet", 1, 10L, null);

        assertThatCode(() -> consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", staged)))
                .doesNotThrowAnyException();
    }
}
