package com.dataplatform.processor.consumers;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.consumers.models.DataEvent;
import com.dataplatform.processor.consumers.tranformers.JsonSerde;
import static com.dataplatform.processor.utils.Constants.Topic.*;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.ArrayList;

@Factory
public class ProcessDataEventsTopology {

    public final String FILE_STORE_NAME = "raw-event-local-store";

    private final PlatformDataConfig platformDataConfig;

    public ProcessDataEventsTopology(PlatformDataConfig platformDataConfig) {
        this.platformDataConfig = platformDataConfig;
    }

    @Context
    public KStream<String, List<DataEvent>> groupRawDataStream(ConfiguredStreamBuilder builder) {
        var dataEventSerde = new JsonSerde<>(DataEvent.class);
        var dataEventListSerde = Serdes.ListSerde(ArrayList.class, dataEventSerde);

        StoreBuilder<KeyValueStore<String, List<DataEvent>>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FILE_STORE_NAME),
                Serdes.String(),
                dataEventListSerde
        );
        builder.addStateStore(storeBuilder);

        KStream<String, DataEvent> stream = builder.stream(DATA_EVENTS, Consumed.with(Serdes.String(), dataEventSerde));
        var transformedStream = stream.process(
                () -> new BufferDataEventsProcessor(
                        FILE_STORE_NAME,
                        platformDataConfig.getMaxBufferRecords(),
                        platformDataConfig.getMaxBufferTime()),
                FILE_STORE_NAME);
        transformedStream.to(STORE_DATA_EVENTS, Produced.with(Serdes.String(), dataEventListSerde));
        return transformedStream;
    }
}
