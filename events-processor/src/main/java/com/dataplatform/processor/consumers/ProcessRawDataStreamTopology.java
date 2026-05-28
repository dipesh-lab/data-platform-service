package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.RawDataEvent;
import com.dataplatform.processor.consumers.tranformers.JsonSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.*;
import java.util.function.Supplier;

@Factory
public class ProcessRawDataStreamTopology {

    public final String FILE_STORE_NAME = "raw-event-local-store";

    @Context
    public KStream<String, List<RawDataEvent>> groupRawDataStream(ConfiguredStreamBuilder builder) {
        var dataEventSerde = new JsonSerde<RawDataEvent>(RawDataEvent.class);
        var dataEventListSerde = Serdes.ListSerde(ArrayList.class, dataEventSerde);

        StoreBuilder<KeyValueStore<String, List<RawDataEvent>>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FILE_STORE_NAME),
                Serdes.String(),
                dataEventListSerde
        );
        builder.addStateStore(storeBuilder);

        KStream<String, RawDataEvent> stream = builder.stream("dp-raw-events", Consumed.with(Serdes.String(), dataEventSerde));
        var transformedStream = stream.process(
                () -> new BufferRecordsProcessor(FILE_STORE_NAME, 1000, "5m"),
                FILE_STORE_NAME);
        transformedStream.to("dp-raw-grouped-events", Produced.with(Serdes.String(), dataEventListSerde));
        return transformedStream;
    }
}
