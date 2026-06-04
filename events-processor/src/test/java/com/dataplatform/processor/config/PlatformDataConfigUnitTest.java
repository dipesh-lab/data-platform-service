package com.dataplatform.processor.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PlatformDataConfigUnitTest {

    @Test
    void settersAndGetters_holdValues() {
        PlatformDataConfig config = new PlatformDataConfig();
        config.setCatalogDir("/catalog-data");
        config.setMaxBufferRecords(2000);
        config.setMaxBufferTime("PT2M");
        config.setPreCommitMergeWindow("PT3M");
        config.setAssumeRoleArn("arn:aws:iam::1:role/glue");
        config.setRegion("ap-southeast-2");

        assertThat(config.getCatalogDir()).isEqualTo("/catalog-data");
        assertThat(config.getMaxBufferRecords()).isEqualTo(2000);
        assertThat(config.getMaxBufferTime()).isEqualTo("PT2M");
        assertThat(config.getPreCommitMergeWindow()).isEqualTo("PT3M");
        assertThat(config.getAssumeRoleArn()).isEqualTo("arn:aws:iam::1:role/glue");
        assertThat(config.getRegion()).isEqualTo("ap-southeast-2");
    }
}
