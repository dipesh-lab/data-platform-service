package com.dataplatform.processor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("app.platform.data")
public class PlatformDataConfig {

    private String folderPath;

    public String getFolderPath() {
        return folderPath;
    }

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }
}
