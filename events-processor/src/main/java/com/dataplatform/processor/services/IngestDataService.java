package com.dataplatform.processor.services;

import com.dataplatform.processor.consumers.models.StoredData;
import org.apache.iceberg.data.GenericRecord;

import java.util.List;

public interface IngestDataService {

    StoredData storeData(String namespace, String type, List<GenericRecord> records) throws Exception;

    StoredData mergeStoredFiles(String namespace, String type, List<StoredData> stagedFiles) throws Exception;

    void commitStageFile(StoredData rawData);
}
