package com.nordstrom.kafka.connect.auth;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AWSUserCredentialsProviderTest {

    private static final String NON_EXISTENT_FIELD_NAME = "non.existent.field.name";

    private ImmutableMap<String, String> testConfigs;

    @Before
    public void setupTestConfigs() {
        testConfigs = new ImmutableMap.Builder<String, String>()
            .put(AWSUserCredentialsProvider.ACCESS_KEY_CONFIG, "test-access-key")
            .put(AWSUserCredentialsProvider.SECRET_KEY_CONFIG, "test-secret-key")
            .build();
    }

    @Test
    public void testConfigureInitializesProviderFields() {
        AWSUserCredentialsProvider testProvider = new AWSUserCredentialsProvider();

        testProvider.configure(testConfigs);

        assertEquals("test-access-key", testProvider.getAccessKey());
        assertEquals("test-secret-key", testProvider.getSecretKey());
    }

    @Test
    public void testGetOptionalFieldReturnsNullGivenNonExistentConfigName() {
        AWSUserCredentialsProvider testProvider = new AWSUserCredentialsProvider();

        testProvider.configure(testConfigs);

        assertNull(testProvider.getOptionalField(testConfigs, NON_EXISTENT_FIELD_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRequiredFieldThrowsExceptionGivenNonExistentConfigName() {
        AWSUserCredentialsProvider testProvider = new AWSUserCredentialsProvider();

        testProvider.configure(testConfigs);

        assertNull(testProvider.getRequiredField(testConfigs, NON_EXISTENT_FIELD_NAME));
    }
}
