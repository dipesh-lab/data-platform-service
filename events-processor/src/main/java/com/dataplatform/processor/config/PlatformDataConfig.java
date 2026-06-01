package com.dataplatform.processor.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("app.platform.data")
public class PlatformDataConfig {

    private String catalogDir;
    private Integer maxBufferRecords;
    private String maxBufferTime;
    private String preCommitMergeWindow;
    private String assumeRoleArn;
    private String region;

    public String getCatalogDir() {
        return catalogDir;
    }

    public void setCatalogDir(String catalogDir) {
        this.catalogDir = catalogDir;
    }

    public Integer getMaxBufferRecords() {
        return maxBufferRecords;
    }

    public void setMaxBufferRecords(Integer maxBufferRecords) {
        this.maxBufferRecords = maxBufferRecords;
    }

    public String getMaxBufferTime() {
        return maxBufferTime;
    }

    public void setMaxBufferTime(String maxBufferTime) {
        this.maxBufferTime = maxBufferTime;
    }

    public String getPreCommitMergeWindow() {
        return preCommitMergeWindow;
    }

    public void setPreCommitMergeWindow(String preCommitMergeWindow) {
        this.preCommitMergeWindow = preCommitMergeWindow;
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
