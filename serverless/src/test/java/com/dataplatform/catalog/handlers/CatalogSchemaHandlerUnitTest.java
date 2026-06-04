package com.dataplatform.catalog.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.dataplatform.catalog.handlers.models.ApplySchemaDTO;
import com.dataplatform.catalog.models.ApplySchema;
import com.dataplatform.catalog.services.AWSGlueCatalogServiceImpl;
import com.dataplatform.catalog.utils.EnvVariables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CatalogSchemaHandlerUnitTest {

    @Mock
    private Context context;

    @Test
    void handleRequest_blankInput_returnsValidationMessage() {
        CatalogSchemaHandler handler = new CatalogSchemaHandler();

        assertThat(handler.handleRequest(new ApplySchemaDTO("", "api_events"), context))
                .isEqualTo("Schema namespace or event name not found");
        assertThat(handler.handleRequest(new ApplySchemaDTO("global", ""), context))
                .isEqualTo("Schema namespace or event name not found");
    }

    @Test
    void handleRequest_success_appliesSchema() throws Exception {
        try (MockedStatic<EnvVariables> envStatic = mockStatic(EnvVariables.class);
             MockedConstruction<AWSGlueCatalogServiceImpl> serviceConstruction = mockConstruction(
                     AWSGlueCatalogServiceImpl.class,
                     (mock, ctx) -> doNothing().when(mock).applySchema(any(ApplySchema.class)))) {
            EnvVariables env = mock(EnvVariables.class);
            envStatic.when(EnvVariables::getInstance).thenReturn(env);
            when(env.getRegion()).thenReturn("ap-southeast-2");
            when(env.getEnvVar("CATALOG_DATA_PATH")).thenReturn("/catalog-data");
            when(env.getEnvVar("GLUE_ASSUME_ROLE_ARN")).thenReturn("arn:aws:iam::1:role/glue");

            CatalogSchemaHandler handler = new CatalogSchemaHandler();
            String result = handler.handleRequest(new ApplySchemaDTO("global", "api_events"), context);

            assertThat(result).isEqualTo("Schema applied");
            AWSGlueCatalogServiceImpl service = serviceConstruction.constructed().getFirst();
            verify(service).applySchema(new ApplySchema("global", "api_events"));
        }
    }

    @Test
    void handleRequest_ioException_returnsMessage() throws Exception {
        try (MockedStatic<EnvVariables> envStatic = mockStatic(EnvVariables.class);
             MockedConstruction<AWSGlueCatalogServiceImpl> serviceConstruction = mockConstruction(
                     AWSGlueCatalogServiceImpl.class,
                     (mock, ctx) -> doThrow(new IOException("glue failed")).when(mock).applySchema(any(ApplySchema.class)))) {
            EnvVariables env = mock(EnvVariables.class);
            envStatic.when(EnvVariables::getInstance).thenReturn(env);
            when(env.getRegion()).thenReturn("ap-southeast-2");
            when(env.getEnvVar("CATALOG_DATA_PATH")).thenReturn("/catalog-data");
            when(env.getEnvVar("GLUE_ASSUME_ROLE_ARN")).thenReturn("arn:aws:iam::1:role/glue");

            CatalogSchemaHandler handler = new CatalogSchemaHandler();
            String result = handler.handleRequest(new ApplySchemaDTO("global", "api_events"), context);

            assertThat(result).isEqualTo("glue failed");
            verify(serviceConstruction.constructed().getFirst()).applySchema(any(ApplySchema.class));
        }
    }
}
