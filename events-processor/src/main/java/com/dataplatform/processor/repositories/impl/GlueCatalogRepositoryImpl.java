package com.dataplatform.processor.repositories.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.models.CatalogSchema;
import com.dataplatform.processor.repositories.CatalogRepository;
import com.dataplatform.processor.utils.JsonUtils;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Singleton
public class GlueCatalogRepositoryImpl implements CatalogRepository {

    private static final Logger log = LoggerFactory.getLogger(GlueCatalogRepositoryImpl.class);

    private final PlatformDataConfig dataConfig;

    public GlueCatalogRepositoryImpl(PlatformDataConfig dataConfig) {
        this.dataConfig = dataConfig;
    }

    @Override
    public Optional<Table> findTable(String namespace, String tableName, String bucketName) throws CatalogSchemaException {
        try (var catalog = new GlueCatalog()) {
            catalog.setConf(new Configuration());
            var bucketUri = "s3://" + namespace + "-" + bucketName;
            catalog.initialize("glue_catalog",
                    Map.of(
                            CatalogProperties.WAREHOUSE_LOCATION, bucketUri,
                            CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName(),
                            CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName(),
                            CatalogProperties.TABLE_DEFAULT_PREFIX, "",
                            AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName(),
                            AwsProperties.CLIENT_ASSUME_ROLE_ARN, dataConfig.getAssumeRoleArn(),
                            AwsProperties.CLIENT_ASSUME_ROLE_REGION, dataConfig.getRegion(),
                            "client.region", dataConfig.getRegion()
                    )
            );
            log.info("Glue catalog initialized");
            var namespaceObj = Namespace.of(namespace);
            var tableId = TableIdentifier.of(namespaceObj, tableName);
            return Optional.of(catalog.loadTable(tableId));
        } catch (NoSuchTableException e) {
            log.warn("Table not found in glue catalog", e);
            return Optional.empty();
        } catch (IOException e) {
            log.error("Error occurred while loading table from catalog. {}", e.getMessage(), e);
            throw new CatalogSchemaException(e.getMessage(), e);
        }
    }

    @Override
    public List<CatalogSchema> listAllSchemas() {
        log.info("Listing all schemas from {} directory", dataConfig.getCatalogDir());
        try (Stream<Path> paths = Files.walk(Paths.get(dataConfig.getCatalogDir()))) {
            return paths.filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .filter(file -> file.getName().endsWith(".json"))
                    .map(file -> JsonUtils.deserializeFile(file, CatalogSchema.class))
                    .toList();
        } catch (IOException e) {
            log.error("Error while reading table metadata", e);
            throw new CatalogSchemaException(e.getMessage(), e);
        }
    }
}