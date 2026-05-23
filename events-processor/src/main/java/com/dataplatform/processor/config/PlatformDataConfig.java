package com.dataplatform.processor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("app.platform.data")
public class PlatformDataConfig {

    private String catalogDir;
    private String assumeRoleArn;
    private String region;

    public String getCatalogDir() {
        return catalogDir;
    }

    public void setCatalogDir(String catalogDir) {
        this.catalogDir = catalogDir;
    }

    public String getAssumeRoleArn() {
        return assumeRoleArn;
    }

    public void setAssumeRoleArn(String assumeRoleArn) {
        this.assumeRoleArn = assumeRoleArn;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
