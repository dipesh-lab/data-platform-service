package com.dataplatform.catalog.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.dataplatform.catalog.config.CatalogConfig;
import com.dataplatform.catalog.handlers.models.ApplySchemaDTO;
import com.dataplatform.catalog.models.ApplySchema;
import com.dataplatform.catalog.services.AWSGlueCatalogServiceImpl;
import com.dataplatform.catalog.utils.EnvVariables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

@Slf4j
public class CatalogSchemaHandler implements RequestHandler<ApplySchemaDTO, String> {

    private final EnvVariables envVars;

    public CatalogSchemaHandler(EnvVariables envVars) {
        this.envVars = envVars;
    }

    @Override
    public String handleRequest(ApplySchemaDTO schemaDto, Context context) {
        if (StringUtils.isBlank(schemaDto.namespace()) || StringUtils.isBlank(schemaDto.eventName())) {
            return "Schema namespace or event name not found";
        }
        var catalogConfig = new CatalogConfig(envVars.getRegion(), envVars.getEnvVar("CATALOG_DATA_PATH"),
                envVars.getEnvVar("GLUE_ASSUME_ROLE_ARN"));
        var applySchema = new ApplySchema(schemaDto.namespace(), schemaDto.eventName());
        var service = new AWSGlueCatalogServiceImpl(catalogConfig);
        try {
            service.applySchema(applySchema);
            return "Schema applied";
        } catch (IOException e) {
            log.error("An error occurred while applying schema", e);
            return e.getMessage();
        }
    }

    public static void main(String[] arg) {
        var handler = new CatalogSchemaHandler(EnvVariables.getInstance());
    }
}