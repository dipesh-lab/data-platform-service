
package com.dataplatform.catalog.services;

import com.dataplatform.catalog.config.CatalogConfig;
import com.dataplatform.catalog.exceptions.CatalogException;
import com.dataplatform.catalog.models.ApplySchema;
import com.dataplatform.catalog.models.CatalogSchema;
import com.dataplatform.catalog.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.required;

@Slf4j
public class AWSGlueCatalogServiceImpl implements CatalogSchemaService {

    private final CatalogConfig catalogConfig;

    public AWSGlueCatalogServiceImpl(CatalogConfig catalogConfig) {
        this.catalogConfig = catalogConfig;
    }

    @Override
    public void applySchema(ApplySchema applySchema) throws IOException {
        String filePath = catalogConfig.schemaFolder() + File.separator + applySchema.namespace() + File.separator + applySchema.tableName() + ".json";
        var schemaFile = new File(filePath);
        if (!schemaFile.exists()) {
            throw new CatalogException("Catalog schema file [" + applySchema.tableName() + ".json] for namespace [" + applySchema.namespace() + "] not exist");
        }
        try (var catalog = new GlueCatalog()) {
            catalog.setConf(new Configuration());
            var bucketUri = "s3://" + applySchema.namespace() + "-" +applySchema.tableName();
            catalog.initialize("glue_catalog",
                    Map.of(
                            CatalogProperties.WAREHOUSE_LOCATION, bucketUri,
                            CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName(),
                            CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName(),
                            CatalogProperties.TABLE_DEFAULT_PREFIX, "",
                            AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName(),
                            AwsProperties.CLIENT_ASSUME_ROLE_ARN, catalogConfig.assumeRoleArn(),
                            AwsProperties.CLIENT_ASSUME_ROLE_REGION, catalogConfig.region(),
                            "client.region", catalogConfig.region()
                    )
            );
            log.info("Glue catalog initialized");
            var namespaceObj = Namespace.of(applySchema.namespace());
            createNamespaceIfNotExist(namespaceObj, catalog);
            var schemaObj = readSchema(schemaFile);
            var tableId = TableIdentifier.of(namespaceObj, schemaObj.tableName());
            createOrUpdateTable(tableId, catalog, schemaObj);
        }
    }

    private void createNamespaceIfNotExist(Namespace namespace, GlueCatalog catalog) {
        if (!catalog.namespaceExists(namespace)) {
            log.info("Namespace with name [{}] not exist hence creating it", namespace.level(0));
            catalog.createNamespace(namespace, Map.of());
            log.info("Namespace with name [{}] created", namespace.level(0));
        } else {
            log.warn("Namespace [{}] already exist", namespace.level(0));
        }
    }

    private void createOrUpdateTable(TableIdentifier tableId, GlueCatalog catalog, CatalogSchema schemaObj) {
        if (catalog.tableExists(tableId)) {
            log.warn("Table [{}] already exist in namespace {}", tableId.name(), tableId.namespace().level(0));
            var table = catalog.loadTable(tableId);
            var columns = table.schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toSet());
            var newColumns = schemaObj.fields().stream().filter(field -> !columns.contains(field.name())).toList();
            log.info("Found {} new columns need to be added", newColumns.size());
            if (CollectionUtils.isNotEmpty(newColumns)) {
                var updateSchema = table.updateSchema();
                newColumns.forEach(field -> updateSchema.addColumn(field.name(), getType(field.dataType())));
                updateSchema.commit();
                log.info("Table is updated with new columns");
            }
        } else {
            log.info("Table with name [{}] not exist in namespace {}, hence creating it", tableId.name(), tableId.namespace().level(0));
            var schema = createSchema(schemaObj);
            var partition = createPartition(schemaObj.partitionType(), schema);
            var table = catalog.createTable(tableId, schema, partition);
            log.info("Data table [{}] created at {} location", table.name(), table.location());
        }
    }

    private CatalogSchema readSchema(File schemaFile) throws IOException {
        try (var stream = new FileInputStream(schemaFile)) {
            return JsonUtils.deserialize(stream, CatalogSchema.class);
        }
    }

    private Schema createSchema(CatalogSchema catalogSchema) {
        var fields = new ArrayList<Types.NestedField>(catalogSchema.fields().size() + 5);
        fields.add(required(1, "event_id", Types.StringType.get()));
        fields.add(required(2, "tenant_id", Types.StringType.get()));
        fields.add(required(3, "load_time", Types.LongType.get()));
        fields.add(required(4, "event_time", Types.TimestampType.withoutZone()));

        for (int i = 0; i< catalogSchema.fields().size(); i++) {
            var field = catalogSchema.fields().get(i);
            var dataType = getType(field.dataType());
            fields.add(required(5 + i, field.name(), dataType));
        }
        return new Schema(fields);
    }

    private PartitionSpec createPartition(CatalogSchema.PartitionType partitionType, Schema schema) {
        var partitionBuilder = PartitionSpec.builderFor(schema);
        if (CatalogSchema.PartitionType.HOUR == partitionType) {
            partitionBuilder.hour("event_time");
        } else {
            partitionBuilder.day("event_time");
        }
        return partitionBuilder.build();
    }

    private Type.PrimitiveType getType(CatalogSchema.DataType type) {
        return switch (type) {
            case INT -> Types.IntegerType.get();
            case LONG -> Types.LongType.get();
            case TIMESTAMP -> Types.TimestampType.withoutZone();
            case DATE -> Types.DateType.get();
            default -> Types.StringType.get();
        };
    }
}
