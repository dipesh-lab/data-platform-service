package com.dataplatform.catalog.utils;

import java.util.Optional;

public final class EnvVariables {

    private static final EnvVariables INSTANCE = new EnvVariables();

    private static final String AWS_REGION = "AWS_REGION";
    private static final String ENV_NAME = "ENV_NAME";

    public static EnvVariables getInstance() {
        return INSTANCE;
    }

    private EnvVariables() {}

    public String getRegion() {
        return System.getenv(AWS_REGION);
    }

    public String getEnvType() {
        return getEnvVar(ENV_NAME);
    }

    public String getEnvVar(String name) {
        return Optional.ofNullable(name)
                .map(System::getenv)
                .orElseThrow(() -> new IllegalArgumentException("Environment variable [" + name + "] not configured"));
    }
}
