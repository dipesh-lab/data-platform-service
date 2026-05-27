package com.dataplatform.processor.services;

import com.dataplatform.processor.consumers.models.StoredRawData;
import org.apache.iceberg.data.GenericRecord;

import java.util.List;

public interface IngestDataService {

    StoredRawData prepare(String namespace, String type, List<GenericRecord> records) throws Exception;

    void writeData(StoredRawData rawData);
}
