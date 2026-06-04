package com.dataplatform.catalog.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvVariablesUnitTest {

    @Test
    void getInstance_returnsSingleton() {
        assertThat(EnvVariables.getInstance()).isSameAs(EnvVariables.getInstance());
    }

    @Test
    void getEnvVar_missingVariable_throws() {
        assertThatThrownBy(() -> EnvVariables.getInstance().getEnvVar("NOT_CONFIGURED_ENV_VAR_XYZ"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NOT_CONFIGURED_ENV_VAR_XYZ");
    }

    @Test
    void getRegion_readsEnvironment() {
        assertThat(EnvVariables.getInstance().getRegion()).isNull();
    }

    @Test
    void getEnvType_missingVariable_throws() {
        assertThatThrownBy(() -> EnvVariables.getInstance().getEnvType())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ENV_NAME");
    }
}
