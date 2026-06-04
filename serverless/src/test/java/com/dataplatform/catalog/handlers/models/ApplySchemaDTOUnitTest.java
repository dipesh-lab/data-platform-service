package com.dataplatform.catalog.handlers.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApplySchemaDTOUnitTest {

    @Test
    void record_holdsValues() {
        ApplySchemaDTO dto = new ApplySchemaDTO("global", "api_events");

        assertThat(dto.namespace()).isEqualTo("global");
        assertThat(dto.tableName()).isEqualTo("api_events");
    }
}
