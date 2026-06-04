package com.dataplatform.catalog.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApplySchemaUnitTest {

    @Test
    void record_holdsValues() {
        ApplySchema applySchema = new ApplySchema("global", "api_events");

        assertThat(applySchema.namespace()).isEqualTo("global");
        assertThat(applySchema.tableName()).isEqualTo("api_events");
    }
}
