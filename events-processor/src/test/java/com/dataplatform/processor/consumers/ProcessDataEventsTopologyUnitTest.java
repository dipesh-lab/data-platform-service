package com.dataplatform.processor.consumers;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.consumers.models.DataEvent;
import com.dataplatform.processor.utils.Constants;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessDataEventsTopologyUnitTest {

    @Mock
    private PlatformDataConfig platformDataConfig;

    private ProcessDataEventsTopology topology;

    @BeforeEach
    void setUp() {
        topology = new ProcessDataEventsTopology(platformDataConfig);
        when(platformDataConfig.getMaxBufferRecords()).thenReturn(10);
        when(platformDataConfig.getMaxBufferTime()).thenReturn("PT1M");
    }

    @Test
    void groupRawDataStream_buildsKafkaStreamsTopology() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "process-data-events-test");
        ConfiguredStreamBuilder builder = new ConfiguredStreamBuilder(props);

        KStream<String, List<DataEvent>> result = topology.groupRawDataStream(builder);

        assertThat(result).isNotNull();
        assertThat(builder.build().describe().subtopologies()).isNotEmpty();
        assertThat(topology.FILE_STORE_NAME).isEqualTo("raw-event-local-store");
        assertThat(Constants.Topic.DATA_EVENTS).isEqualTo("dp-data-events");
    }
}
