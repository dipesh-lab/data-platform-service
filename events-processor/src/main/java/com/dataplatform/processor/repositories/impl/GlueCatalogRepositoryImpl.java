package com.dataplatform.processor.repositories.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.exceptions.CatalogSchemaException;
import com.dataplatform.processor.repositories.CatalogRepository;
import com.dataplatform.processor.repositories.TableSchemaRepository;
import jakarta.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Singleton
public class GlueCatalogRepositoryImpl implements CatalogRepository {

    private static final Logger log = LoggerFactory.getLogger(GlueCatalogRepositoryImpl.class);

    private final PlatformDataConfig dataConfig;
    private final TableSchemaRepository tableSchemaRepository;

    public GlueCatalogRepositoryImpl(PlatformDataConfig dataConfig,
                                     TableSchemaRepository tableSchemaRepository) {
        this.dataConfig = dataConfig;
        this.tableSchemaRepository = tableSchemaRepository;
    }

    @Override
    public GlueCatalog getCatalog(String namespace, String tableName) throws CatalogSchemaException {
        return tableSchemaRepository.findBucketName(namespace, tableName)
                .map(bucketName -> {
                    var catalog = new GlueCatalog();
                    catalog.setConf(new Configuration());
                    var bucketUri = "s3://" + bucketName + "/";
                    log.info("Catalog S3 bucket [{}]", bucketUri);
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
                    log.info("Catalog initialized for table {} and namespace {}", tableName, namespace);
                    return catalog;
                }).orElseThrow(() ->
                        new CatalogSchemaException("Bucket not found for table [" + tableName + "] and [" + namespace + "] namespace"));
    }
}