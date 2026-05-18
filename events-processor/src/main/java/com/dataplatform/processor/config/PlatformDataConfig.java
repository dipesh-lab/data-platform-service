package com.dataplatform.processor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("app.platform.data")
public class PlatformDataConfig {

    private String catalogDir;

    public String getCatalogDir() {
        return catalogDir;
    }

    public void setCatalogDir(String catalogDir) {
        this.catalogDir = catalogDir;
    }
}
