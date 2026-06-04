package com.dataplatform.catalog.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogConfigUnitTest {

    @Test
    void record_holdsValues() {
        CatalogConfig config = new CatalogConfig("ap-southeast-2", "/catalog-data", "arn:aws:iam::1:role/glue");

        assertThat(config.region()).isEqualTo("ap-southeast-2");
        assertThat(config.schemaFolder()).isEqualTo("/catalog-data");
        assertThat(config.assumeRoleArn()).isEqualTo("arn:aws:iam::1:role/glue");
    }
}
