package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.services.IngestDataService;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class MergeDataEventsProcessor implements Processor<String, StoredData, String, StoredData> {

    private static final Logger log = LoggerFactory.getLogger(MergeDataEventsProcessor.class);

    private final String storeName;
    private final Duration mergeWindow;
    private final IngestDataService ingestDataService;

    private ProcessorContext<String, StoredData> context;
    private KeyValueStore<String, List<StoredData>> kvStore;

    public MergeDataEventsProcessor(String storeName,
                                    Duration mergeWindow,
                                    IngestDataService ingestDataService) {
        this.storeName = storeName;
        this.mergeWindow = mergeWindow;
        this.ingestDataService = ingestDataService;
    }

    @Override
    public void init(ProcessorContext<String, StoredData> context) {
        this.context = context;
        this.kvStore = context.getStateStore(storeName);
        log.info("Initializing merge staged data processor with store {} and window {}", storeName, mergeWindow);
        context.schedule(mergeWindow, PunctuationType.WALL_CLOCK_TIME, this::flushAll);
    }

    @Override
    public void process(Record<String, StoredData> record) {
        var staged = record.value();
        if (staged == null || StringUtils.isAnyBlank(staged.namespace(), staged.type(), staged.location())) {
            return;
        }
        var key = resolveKey(staged);
        var kValue = kvStore.get(key);
        var list = Optional.ofNullable(kValue).map(ArrayList::new).orElse(new ArrayList<StoredData>(1));
        list.add(staged);
        kvStore.put(key, list);
        log.info("Staged catalog write queued for key {}, total pending files {}", key, list.size());
    }

    private void flushAll(long timestamp) {
        log.info("Pre-commit merge punctuator triggered at {}", timestamp);
        var keys = new HashSet<String>();
        try (var itr = kvStore.all()) {
            while (itr.hasNext()) {
                var kv = itr.next();
                flushKey(kv.key, List.copyOf(kv.value), timestamp);
                keys.add(kv.key);
            }
            keys.forEach(kvStore::delete);
        }
    }

    private void flushKey(String key, List<StoredData> stagedFiles, long timestamp) {
        var namespace = stagedFiles.getFirst().namespace();
        var type = stagedFiles.getFirst().type();
        try {
            var merged = ingestDataService.mergeStoredFiles(namespace, type, stagedFiles);
            if (null != merged) {
                log.info("Pre-commit merge produced {} of key {} ({} source files)", merged.location(), key, stagedFiles.size());
                context.forward(new Record<>(key, merged, timestamp));
            }
        } catch (Exception e) {
            log.error("Pre-commit merge failed for key {} with {} staged files", key, stagedFiles.size(), e);
        }
    }

    private static String resolveKey(StoredData staged) {
        return staged.namespace() + "|" + staged.type();
    }
}
