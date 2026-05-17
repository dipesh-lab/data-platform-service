package com.dataplatform.catalog.services;

import com.dataplatform.catalog.models.ApplySchema;

import java.io.IOException;

public interface CatalogSchemaService {

    void applySchema(ApplySchema applySchema) throws IOException;
}
