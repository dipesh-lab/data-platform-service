package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.services.IngestDataService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class CommitDataEventsConsumerUnitTest {

    @Mock
    private IngestDataService ingestDataService;

    @InjectMocks
    private CommitDataEventsConsumer consumer;

    @Test
    void receive_nullValue_skipsCommit() {
        consumer.receive(new ConsumerRecord<>("k", 0, 0, "k", null));

        verifyNoInteractions(ingestDataService);
    }

    @Test
    void receive_validValue_commitsStageFile() {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/file.parquet", 3, 100L, null);

        consumer.receive(new ConsumerRecord<>("global|api_events", 0, 0, "global|api_events", staged));

        verify(ingestDataService).commitStageFile(staged);
    }
}
