package com.dataplatform.processor.consumers;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.services.IngestDataService;
import com.dataplatform.processor.utils.Constants;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MergeDataEventsTopologyUnitTest {

    @Mock
    private PlatformDataConfig platformDataConfig;
    @Mock
    private IngestDataService ingestDataService;

    private MergeDataEventsTopology topology;

    @BeforeEach
    void setUp() {
        topology = new MergeDataEventsTopology(platformDataConfig, ingestDataService);
        when(platformDataConfig.getPreCommitMergeWindow()).thenReturn("PT3M");
    }

    @Test
    void catalogWrittenMergeStream_buildsKafkaStreamsTopology() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-data-events-test");
        ConfiguredStreamBuilder builder = new ConfiguredStreamBuilder(props);

        KStream<String, StoredData> result = topology.catalogWrittenMergeStream(builder);

        assertThat(result).isNotNull();
        assertThat(builder.build().describe().subtopologies()).isNotEmpty();
        assertThat(MergeDataEventsTopology.STORE_NAME).isEqualTo("merge-staged-data-files-store");
        assertThat(Constants.Topic.MERGE_DATA_EVENTS).isEqualTo("dp-merge-data-events");
    }
}
