package com.dataplatform.processor.services.impl;

import com.dataplatform.processor.consumers.models.StoredRawData;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.exceptions.CatalogTableNotFoundException;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.CatalogSchemaRegistry;
import com.dataplatform.processor.services.IngestDataService;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Singleton
public class IngestRawDataServiceImpl implements IngestDataService {

    private static final Logger log = LoggerFactory.getLogger(IngestRawDataServiceImpl.class);

    private final CatalogSchemaRegistry schemaRegistry;

    @Inject
    public IngestRawDataServiceImpl(CatalogSchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public StoredRawData prepare(String namespace, String type, List<GenericRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            return null;
        }
        try {
            var table = resolveTable(namespace, type);
            String fileName = UUID.randomUUID() + ".parquet";
            try (var io = table.io()) {
                var outputFile = io.newOutputFile(table.location() + "/data/" + fileName);
                FileAppender<GenericRecord> fileAppender = Parquet.write(outputFile)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::create)
                        .build();
                records.forEach(fileAppender::add);
                fileAppender.close();
                var storedRecords = new StoredRawData(namespace, type, outputFile.location(), records.size(), fileAppender.length());
                log.info("Staged parquet file {} for type {} with {} records ({} bytes)",
                        storedRecords.location(),
                        type,
                        storedRecords.totalRecords(),
                        storedRecords.length());
                return storedRecords;
            } catch (IOException e) {
                log.error("Error occurred while writing parquet files", e);
                throw new RetryException(e.getMessage(), e);
            }
        } catch (CatalogTableNotFoundException e) {
            log.error("Catalog table [{}] not found in [{}] namespace", type, namespace, e);
            throw new CatalogSchemaException("Catalog table not found: " + e.getMessage(), e);
        }
    }

    @Override
    public void writeData(StoredRawData rawData) {
        if (null == rawData || StringUtils.isBlank(rawData.type()) || StringUtils.isBlank(rawData.location()) ||
                rawData.totalRecords() <= 0 || rawData.length() <= 0) {
            log.warn("Required parquet location, type of size not found, abort processing");
            return;
        }
        log.info("Fetch iceberg table for type {}", rawData.type());
        try {
            var table = resolveTable(rawData.namespace(), rawData.type());
            log.info("Add iceberg metadata with created parquet data. Location {}, total records {}, file size {}",
                    rawData.location(), rawData.totalRecords(), rawData.length());
            DataFile dataFile = DataFiles.builder(table.spec())
                    .withPath(rawData.location())
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(rawData.totalRecords())
                    .withFileSizeInBytes(rawData.length())
                    .build();
            table.newAppend()
                    .appendFile(dataFile)
                    .commit();
        } catch(Exception e) {
            log.error("Error during update table metadata");
        }
    }

    private Table resolveTable(String namespace, String type) {
        return schemaRegistry.getTable(namespace, type);
    }

}