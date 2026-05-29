package com.dataplatform.processor.consumers;

import com.dataplatform.processor.consumers.models.RawDataEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferRecordsProcessor implements Processor<String, RawDataEvent, String, List<RawDataEvent>> {

    private static final Logger log = LoggerFactory.getLogger(BufferRecordsProcessor.class);

    private final String fileStoreName;
    private final Integer maxBufferRecords;
    private final Duration bufferTime;

    private ProcessorContext<String, List<RawDataEvent>> context;
    private KeyValueStore<String, List<RawDataEvent>> kvStore;

    public BufferRecordsProcessor(String fileStoreName, Integer maxBufferRecords, String maxBufferTime) {
        this.fileStoreName = fileStoreName;
        this.maxBufferRecords = maxBufferRecords;
        this.bufferTime = Duration.parse(maxBufferTime);
    }

    @Override
    public void process(Record<String, RawDataEvent> record) {
        var event = record.value();
        if (null != record.value() && StringUtils.isNotEmpty(event.type())) {
            var existing = kvStore.get(event.type());
            var list = null != existing ? new ArrayList<RawDataEvent>(existing) : new ArrayList<RawDataEvent>(1);
            list.add(event);
            kvStore.put(event.type(), list);
            log.info("New event of [{}:{}] type added to buffer", event.namespace(), event.type());
            if (list.size() >= maxBufferRecords) {
                log.info("Buffered records reached to limit {}", maxBufferRecords);
                kvStore.delete(event.type());
                var copiedList = List.copyOf(list);
                log.info("Total {} events forwarding", copiedList.size());
                context.forward(new Record<>(event.type(), copiedList, record.timestamp()));
            }
        }
    }

    @Override
    public void init(ProcessorContext<String, List<RawDataEvent>> context) {
        this.context = context;
        log.info("Initializing processor with store {}", fileStoreName);
        this.kvStore = context.getStateStore(fileStoreName);
        Punctuator punctuator = timestamp -> {
            log.info("Punctuator triggered at {}", timestamp);
            try (var itr = kvStore.all()) {
                var records = new ArrayList<Record<String, List<RawDataEvent>>>(5);
                while(itr.hasNext()) {
                    var kv = itr.next();
                    var copiedList = List.copyOf(kv.value);
                    log.info("Punctuator forwarding {} events", copiedList.size());
                    records.add(new Record<>(kv.key, copiedList, timestamp));
                }
                records.forEach(rd -> kvStore.delete(rd.key()));
                records.forEach(context::forward);
            }
        };
        context.schedule(bufferTime, PunctuationType.WALL_CLOCK_TIME, punctuator);
        log.info("Scheduled punctuator every 60s");
    }
}