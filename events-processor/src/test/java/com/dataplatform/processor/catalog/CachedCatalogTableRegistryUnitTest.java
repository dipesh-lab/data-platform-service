package com.dataplatform.processor.catalog;

import com.dataplatform.processor.repositories.CatalogRepository;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachedCatalogTableRegistryUnitTest {

    @Mock
    private CatalogRepository catalogRepository;

    @Mock
    private GlueCatalog glueCatalog;

    @Mock
    private Table table;

    @Test
    void getTable_loadsFromRepositoryAndCachesResult() throws Exception {
        when(catalogRepository.getCatalog("global", "api_events")).thenReturn(glueCatalog);
        when(glueCatalog.loadTable(TableIdentifier.of(Namespace.of("global"), "api_events"))).thenReturn(table);

        CachedCatalogTableRegistry registry = new CachedCatalogTableRegistry(catalogRepository);

        assertThat(registry.getTable("global", "api_events")).isSameAs(table);
        assertThat(registry.getTable("global", "api_events")).isSameAs(table);

        verify(catalogRepository, times(1)).getCatalog("global", "api_events");
        verify(glueCatalog).loadTable(any(TableIdentifier.class));
    }
}
