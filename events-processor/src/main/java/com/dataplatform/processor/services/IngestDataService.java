package com.dataplatform.processor.services;

import com.dataplatform.processor.consumers.models.StoredRawData;
import org.apache.iceberg.data.GenericRecord;

import java.util.List;

public interface IngestDataService {

    StoredRawData prepare(String type, List<GenericRecord> records);

    void writeData(StoredRawData rawData);
}
