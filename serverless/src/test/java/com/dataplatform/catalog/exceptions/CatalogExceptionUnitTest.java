package com.dataplatform.catalog.exceptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogExceptionUnitTest {

    @Test
    void carriesMessage() {
        CatalogException exception = new CatalogException("schema missing");

        assertThat(exception).isInstanceOf(RuntimeException.class);
        assertThat(exception.getMessage()).isEqualTo("schema missing");
    }
}
