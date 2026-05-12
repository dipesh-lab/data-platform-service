package com.dataplatform.events.processor.config;

import com.dataplatform.events.processor.services.DataCatalogService;
import com.dataplatform.events.processor.services.RawDataService;
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