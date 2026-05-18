package com.dataplatform.processor.services.impl;

import com.dataplatform.processor.config.PlatformDataConfig;
import com.dataplatform.processor.services.CatalogDataService;
import jakarta.inject.Singleton;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class GlueCatalogDataServiceImpl implements CatalogDataService {

    private static final Logger log = LoggerFactory.getLogger(GlueCatalogDataServiceImpl.class);

    private final PlatformDataConfig dataConfig;

    public GlueCatalogDataServiceImpl(PlatformDataConfig dataConfig) {
        this.dataConfig = dataConfig;
    }

    @Override
    public Map<String, Table> getTables() {
        var tables = new HashMap<String, Table>();
        // To-Do
        return tables;
    }
}
