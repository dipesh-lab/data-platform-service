package com.dataplatform.processor.config;

import com.dataplatform.processor.services.CatalogDataService;
import com.dataplatform.processor.services.impl.IngestRawDataServiceImpl;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;

@Factory
public class ApplicationConfig {

    @Bean
    public IngestRawDataServiceImpl ingestRawDataService(CatalogDataService catalogService) {
        return new IngestRawDataServiceImpl(catalogService.getTables());
    }

}