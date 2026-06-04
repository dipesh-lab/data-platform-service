package com.dataplatform.producer;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;

class TestEventDataUnitTest {

    @Test
    void testPools_arePopulated() {
        assertThat(TestEventData.ACCOUNT_IDS).hasSize(5);
        assertThat(TestEventData.USER_IDS).hasSize(5);
        assertThat(TestEventData.OP_TYPES).isNotEmpty();
    }

    @Test
    void privateConstructor_canBeInvoked() throws Exception {
        Constructor<TestEventData> constructor = TestEventData.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        assertThat(constructor.newInstance()).isNotNull();
    }
}
