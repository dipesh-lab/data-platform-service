package com.dataplatform.processor.catalog;

import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.repositories.CatalogRepository;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import jakarta.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Singleton
public class CachedCatalogTableRegistry {

    private static final Logger log = LoggerFactory.getLogger(CachedCatalogTableRegistry.class);

    private final LoadingCache<String, Pair<Table, GlueCatalog>> catalogTableCache;

    private final CatalogRepository catalogRepository;

    public CachedCatalogTableRegistry(CatalogRepository catalogRepository) {
        this.catalogRepository = catalogRepository;
        this.catalogTableCache = initCache();
    }

    public Table getTable(String namespace, String tableName) throws CatalogSchemaException, ExecutionException {
        return catalogTableCache.get(namespace + "|" + tableName).getLeft();
    }

    private LoadingCache<String, Pair<Table, GlueCatalog>> initCache() {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(30))
                .removalListener(removeListener())
                .build(new CacheLoader<>() {
                    @Override
                    public Pair<Table, GlueCatalog> load(String key) throws Exception {
                        var parts = key.split("\\|");
                        var namespace = parts[0];
                        var tableName = parts[1];
                        log.info("Cache miss for key {}, table {}, namespace {}", key, tableName, namespace);
                        try {
                            var catalog = catalogRepository.getCatalog(namespace, tableName);
                            var tableId = TableIdentifier.of(Namespace.of(namespace), tableName);
                            var table = catalog.loadTable(tableId);
                            return Pair.of(table, catalog);
                        } catch (Throwable t) {
                            log.error("Failed to load Iceberg table [{}/{}] - [{}]: {}",
                                    namespace, tableName, t.getClass().getName(), t.getMessage(), t);
                            throw t instanceof Exception ex ? ex : new RuntimeException(t);
                        }
                    }
                });
    }

    private RemovalListener<String, Pair<Table, GlueCatalog>> removeListener() {
        return notification -> {
            log.info("Catalog table remove event for key {}", notification.getKey());
            var pair = notification.getValue();
            IOUtils.closeQuietly(pair.getRight());
        };
    }

    public static void main(String[] arg) {

    }
}
