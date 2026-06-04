package com.dataplatform.processor.services.impl;

import com.dataplatform.processor.catalog.CachedCatalogTableRegistry;
import com.dataplatform.processor.consumers.models.StoredData;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IngestRawDataServiceImplUnitTest {

    @Mock
    private CachedCatalogTableRegistry cachedCatalogTableRegistry;

    @InjectMocks
    private IngestRawDataServiceImpl ingestRawDataService;

    @Test
    void storeData_emptyRecords_returnsNull() throws Exception {
        assertThat(ingestRawDataService.storeData("global", "api_events", List.of())).isNull();

        verifyNoInteractions(cachedCatalogTableRegistry);
    }

    @Test
    void mergeStoredFiles_emptyList_returnsNull() throws Exception {
        assertThat(ingestRawDataService.mergeStoredFiles("global", "api_events", List.of())).isNull();
    }

    @Test
    void mergeStoredFiles_singleFile_returnsSameReference() throws Exception {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/data/a.parquet", 10, 100L, null);

        StoredData result = ingestRawDataService.mergeStoredFiles("global", "api_events", List.of(staged));

        assertThat(result).isSameAs(staged);
        verifyNoInteractions(cachedCatalogTableRegistry);
    }

    @Test
    void commitStageFile_invalidData_skipsCatalogLookup() {
        ingestRawDataService.commitStageFile(null);
        ingestRawDataService.commitStageFile(new StoredData("global", "", "s3://bucket/x.parquet", 1, 1L, null));
        ingestRawDataService.commitStageFile(new StoredData("global", "api_events", "s3://bucket/x.parquet", 0, 1L, null));

        verifyNoInteractions(cachedCatalogTableRegistry);
    }

    @Test
    void commitStageFile_validData_appendsFileToTable() throws Exception {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/data/file.parquet", 5, 512L, null);
        Table table = org.mockito.Mockito.mock(Table.class);
        PartitionSpec spec = org.mockito.Mockito.mock(PartitionSpec.class);
        AppendFiles appendFiles = org.mockito.Mockito.mock(AppendFiles.class);

        when(cachedCatalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.spec()).thenReturn(spec);
        when(table.newAppend()).thenReturn(appendFiles);
        when(appendFiles.appendFile(any(DataFile.class))).thenReturn(appendFiles);

        ingestRawDataService.commitStageFile(staged);

        verify(cachedCatalogTableRegistry).getTable("global", "api_events");
        verify(appendFiles).appendFile(any(DataFile.class));
        verify(appendFiles).commit();
    }

    @Test
    void commitStageFile_appendFailure_isHandled() throws Exception {
        StoredData staged = new StoredData("global", "api_events", "s3://bucket/data/file.parquet", 5, 512L, null);
        Table table = org.mockito.Mockito.mock(Table.class);
        PartitionSpec spec = org.mockito.Mockito.mock(PartitionSpec.class);

        when(cachedCatalogTableRegistry.getTable("global", "api_events")).thenReturn(table);
        when(table.spec()).thenReturn(spec);
        when(table.newAppend()).thenThrow(new RuntimeException("append failed"));

        ingestRawDataService.commitStageFile(staged);

        verify(cachedCatalogTableRegistry).getTable("global", "api_events");
        verify(table).newAppend();
    }
}
