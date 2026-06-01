package com.dataplatform.processor.services.impl;

import com.dataplatform.processor.catalog.CachedCatalogTableRegistry;
import com.dataplatform.processor.consumers.models.StoredData;
import com.dataplatform.processor.exceptions.RetryException;
import com.dataplatform.processor.services.IngestDataService;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.*;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Singleton
public class IngestRawDataServiceImpl implements IngestDataService {

    private static final Logger log = LoggerFactory.getLogger(IngestRawDataServiceImpl.class);

    private final CachedCatalogTableRegistry cachedCatalogTableRegistry;

    public IngestRawDataServiceImpl(CachedCatalogTableRegistry cachedCatalogTableRegistry) {
        this.cachedCatalogTableRegistry = cachedCatalogTableRegistry;
    }

    @Override
    public StoredData storeData(String namespace, String type, List<GenericRecord> records) throws Exception {
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
                var storedRecords = new StoredData(namespace, type, outputFile.location(), records.size(), fileAppender.length(), null);
                log.info("Data stored at [{}] of type [{}:{}] with {} records ({} bytes)",
                        storedRecords.location(),
                        namespace,
                        type,
                        storedRecords.totalRecords(),
                        storedRecords.length());
                return storedRecords;
            } catch (IOException e) {
                log.error("Error occurred while writing parquet files", e);
                throw new RetryException(e.getMessage(), e);
            }
        } catch (RetryException e) {
            throw e;
        } catch (Exception e) {
            log.error("An error occurred while preparing data for [{}] table, [{}] namespace", type, namespace, e);
            throw e;
        }
    }

    @Override
    public StoredData mergeStoredFiles(String namespace, String type, List<StoredData> stagedFiles) throws Exception {
        if (CollectionUtils.isEmpty(stagedFiles)) {
            return null;
        }
        if (stagedFiles.size() == 1) {
            return stagedFiles.getFirst();
        }
        var table = resolveTable(namespace, type);
        String fileName = UUID.randomUUID() + ".parquet";
        try (var io = table.io()) {
            var outputFile = io.newOutputFile(table.location() + "/data/" + fileName);
            FileAppender<GenericRecord> fileAppender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create)
                    .build();
            int totalEvents = 0;
            var mergedSources = new ArrayList<String>(stagedFiles.size());
            for (StoredData staged : stagedFiles) {
                mergedSources.add(staged.location());
                totalEvents += readIntoAppender(io, table.schema(), staged.location(), fileAppender);
            }
            fileAppender.close();
            var merged = new StoredData(namespace, type, outputFile.location(), totalEvents, fileAppender.length(), mergedSources);
            log.info("Pre-commit merged {} staged files into {} ({} records, {} bytes)",
                    stagedFiles.size(), merged.location(), merged.totalRecords(), merged.length());
            return merged;
        } catch (IOException e) {
            log.error("Error merging staged parquet files for [{}/{}]", namespace, type, e);
            throw new RetryException(e.getMessage(), e);
        } catch (Exception e) {
            log.error("An error occurred while merging staged files for [{}/{}]", namespace, type, e);
            throw e;
        }
    }

    private int readIntoAppender(FileIO tableIO, Schema schema, String location, FileAppender<GenericRecord> fileAppender)
            throws IOException {
        InputFile inputFile = tableIO.newInputFile(location);
        int recordCount = 0;
        try (CloseableIterable<GenericRecord> records = Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {
            for (GenericRecord record : records) {
                fileAppender.add(record);
                recordCount++;
            }
        }
        return recordCount;
    }

    /*private void deleteStagedFiles(Table table, List<String> locations) {
        for (String location : locations) {
            try {
                table.io().deleteFile(location);
                log.info("Deleted staged parquet file {}", location);
            } catch (Exception e) {
                log.warn("Failed to delete staged parquet file {}", location, e);
            }
        }
    }*/

    @Override
    public void commitStageFile(StoredData rawData) {
        if (null == rawData || StringUtils.isBlank(rawData.type()) || StringUtils.isBlank(rawData.location()) ||
                rawData.totalRecords() <= 0 || rawData.length() <= 0) {
            log.warn("Required parquet location, type of size not found, abort processing");
            return;
        }
        log.info("Fetch iceberg table for type {}", rawData.type());
        try {
            var table = resolveTable(rawData.namespace(), rawData.type());
            log.info("Commit file [{}] to iceberg format, records {}, file size {}", rawData.location(), rawData.totalRecords(), rawData.length());
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
            log.error("Error during commiting table data", e);
        }
    }

    private Table resolveTable(String namespace, String type) throws ExecutionException {
        return cachedCatalogTableRegistry.getTable(namespace, type);
    }
}