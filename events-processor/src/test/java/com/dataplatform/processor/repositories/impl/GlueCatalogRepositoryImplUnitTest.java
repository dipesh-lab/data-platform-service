package com.dataplatform.processor.repositories.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.repositories.TableSchemaRepository;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GlueCatalogRepositoryImplUnitTest {

    @Mock
    private PlatformDataConfig dataConfig;

    @Mock
    private TableSchemaRepository tableSchemaRepository;

    @InjectMocks
    private GlueCatalogRepositoryImpl repository;

    @Test
    void getCatalog_missingBucket_throwsCatalogSchemaException() {
        when(tableSchemaRepository.findBucketName("global", "missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> repository.getCatalog("global", "missing"))
                .isInstanceOf(CatalogSchemaException.class)
                .hasMessageContaining("Bucket not found");
    }

    @Test
    void getCatalog_initializesGlueCatalogWithBucketAndRole() throws CatalogSchemaException {
        when(tableSchemaRepository.findBucketName("global", "api_events"))
                .thenReturn(Optional.of("global-api-events"));
        when(dataConfig.getAssumeRoleArn()).thenReturn("arn:aws:iam::1:role/glue");
        when(dataConfig.getRegion()).thenReturn("ap-southeast-2");

        try (MockedConstruction<GlueCatalog> ignored = mockConstruction(GlueCatalog.class)) {
            GlueCatalog catalog = repository.getCatalog("global", "api_events");

            verify(catalog).initialize(eq("glue_catalog"), anyMap());
        }
    }
}
