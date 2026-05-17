package com.dataplatform.processor.services;

import com.dataplatform.processor.config.PlatformDataConfig;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.required;

@Singleton
public class DataCatalogService {

    private static final Logger log = LoggerFactory.getLogger(DataCatalogService.class);

    private final PlatformDataConfig dataConfig;

    public DataCatalogService(PlatformDataConfig dataConfig) {
        this.dataConfig = dataConfig;
    }

    public Map<String, Table> getTables() {
        var tables = new HashMap<String, Table>();
        try (var catalog = new HadoopCatalog(new Configuration(), dataConfig.getFolderPath())) {
            var namespace = Namespace.of("dp-applications");
            var tableId = TableIdentifier.of(namespace, "app_events");
            tables.put("app_events", catalog.loadTable(tableId));
        } catch (IOException e) {
            log.error("Error during fetching iceberg tables", e);
        }
        return tables;
    }

    public void createCatalog() throws IOException {
        log.info("Data warehouse path {}", dataConfig.getFolderPath());
        if (StringUtils.isEmpty(dataConfig.getFolderPath())) {
            return;
        }
        try (var catalog = new HadoopCatalog(new Configuration(), dataConfig.getFolderPath())) {

            var namespace = Namespace.of("dp-applications");
            if (!catalog.namespaceExists(namespace)) {
                log.info("Namespace isnt exist, creating it");
                catalog.createNamespace(namespace);
            }
            var tableId = TableIdentifier.of(namespace, "app_events");
            if (!catalog.tableExists(tableId)) {
                log.info("Table isnt exist, creating it");
                var schema = createSchema();
                var partitionSpec = createPartition(schema);
                catalog.createTable(tableId, schema, partitionSpec);
            }
        } catch (IOException e) {
            log.error("Error during catalog creation", e);
            throw e;
        }
    }

    private Schema createSchema() {
        return new Schema(
                required(1, "event_id", Types.StringType.get()),
                required(2, "tenant_id", Types.StringType.get()),
                required(3, "load_time", Types.LongType.get()),
                required(4, "event_time", Types.TimestampType.withoutZone()),
                required(5, "user_id", Types.StringType.get()),
                required(6, "op_type", Types.StringType.get()),
                required(7, "result", Types.StringType.get()),
                required(8, "error", Types.StringType.get())
        );
    }

    private PartitionSpec createPartition(Schema schema) {
        return PartitionSpec.builderFor(schema)
                .day("event_time")
                .build();
    }
}