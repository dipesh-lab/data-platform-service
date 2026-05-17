package com.dataplatform.processor.config;

import com.dataplatform.processor.services.DataCatalogService;
import com.dataplatform.processor.services.RawDataService;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;

import java.io.IOException;

@Factory
public class ApplicationConfig {

    @Bean
    public RawDataService dataWarehouseService(DataCatalogService catalogService) throws IOException {
        catalogService.createCatalog();
        return new RawDataService(catalogService.getTables());
    }

}