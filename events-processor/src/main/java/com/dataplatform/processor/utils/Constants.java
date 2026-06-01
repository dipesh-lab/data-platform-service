package com.dataplatform.processor.utils;

public final class Constants {

    private Constants() {}

    public static class Topic {
        public static final String DATA_EVENTS = "dp-data-events";
        public static final String STORE_DATA_EVENTS = "dp-store-data-events";
        public static final String MERGE_DATA_EVENTS = "dp-merge-data-events";
        public static final String COMMIT_DATA_EVENTS = "dp-commit-data-events";
    }
}
