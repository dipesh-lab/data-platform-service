package com.dataplatform.processor.services;

import com.dataplatform.processor.repositories.CatalogRepository;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;

@Singleton
public class AppStartupListener implements ApplicationEventListener<ServerStartupEvent> {

    private final CatalogSchemaRegistry schemaRegistry;
    private final CatalogRepository catalogRepository;

    @Inject
    public AppStartupListener(CatalogSchemaRegistry schemaRegistry,
                              CatalogRepository catalogRepository) {
        this.schemaRegistry = schemaRegistry;
        this.catalogRepository = catalogRepository;
    }

    @SneakyThrows
    @Override
    public void onApplicationEvent(ServerStartupEvent event) {
        schemaRegistry.loadTableBucketNames(catalogRepository.listAllSchemas());
    }
}
