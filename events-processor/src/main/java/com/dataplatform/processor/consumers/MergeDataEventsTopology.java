package com.dataplatform.processor.consumers;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.consumers.tranformers.JsonSerde;
import com.dataplatform.processor.services.IngestDataService;
import static com.dataplatform.processor.utils.Constants.Topic.*;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Factory
public class MergeDataEventsTopology {

    public static final String STORE_NAME = "merge-staged-data-files-store";

    private final PlatformDataConfig platformDataConfig;
    private final IngestDataService ingestDataService;

    public MergeDataEventsTopology(PlatformDataConfig platformDataConfig,
                                   IngestDataService ingestDataService) {
        this.platformDataConfig = platformDataConfig;
        this.ingestDataService = ingestDataService;
    }

    @Context
    public KStream<String, StoredData> catalogWrittenMergeStream(ConfiguredStreamBuilder builder) {
        var storedRawDataSerde = new JsonSerde<>(StoredData.class);
        var stagedListSerde = Serdes.ListSerde(ArrayList.class, storedRawDataSerde);

        StoreBuilder<KeyValueStore<String, List<StoredData>>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                stagedListSerde
        );
        builder.addStateStore(storeBuilder);

        Duration mergeWindow = Duration.parse(platformDataConfig.getPreCommitMergeWindow());

        KStream<String, StoredData> stream = builder.stream(MERGE_DATA_EVENTS,
                Consumed.with(Serdes.String(), storedRawDataSerde));

        KStream<String, StoredData> mergedStream = stream.process(
                () -> new MergeDataEventsProcessor(STORE_NAME, mergeWindow, ingestDataService), STORE_NAME);

        mergedStream.to(COMMIT_DATA_EVENTS, Produced.with(Serdes.String(), storedRawDataSerde));
        return mergedStream;
    }
}
