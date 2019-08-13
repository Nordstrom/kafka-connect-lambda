package com.nordstrom.kafka.connect.auth;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AWSAssumeRoleCredentialsProviderTest {

    private static final String NON_EXISTENT_FIELD_NAME = "non.existent.field.name";

    private ImmutableMap<String, String> testConfigs;

    @Before
    public void setupTestConfigs() {
        testConfigs = new ImmutableMap.Builder<String, String>()
            .put(AWSAssumeRoleCredentialsProvider.EXTERNAL_ID_CONFIG, "test-external-id")
            .put(AWSAssumeRoleCredentialsProvider.ROLE_ARN_CONFIG, "arn:aws:iam::123456789012:role/test-role")
            .put(AWSAssumeRoleCredentialsProvider.SESSION_NAME_CONFIG, "test-session-name")
            .build();
    }

    @Test
    public void testConfigureInitializesProviderFields() {
        AWSAssumeRoleCredentialsProvider testProvider = new AWSAssumeRoleCredentialsProvider();

        testProvider.configure(testConfigs);

        assertEquals("test-external-id", testProvider.getExternalId());
        assertEquals("arn:aws:iam::123456789012:role/test-role", testProvider.getRoleArn());
        assertEquals("test-session-name", testProvider.getSessionName());
    }

    @Test
    public void testGetOptionalFieldReturnsNullGivenNonExistentConfigName() {
        AWSAssumeRoleCredentialsProvider testProvider = new AWSAssumeRoleCredentialsProvider();

        testProvider.configure(testConfigs);

        assertNull(testProvider.getOptionalField(testConfigs, NON_EXISTENT_FIELD_NAME));
    }

    @Test
    public void testGetOptionalFieldReturnsValueGivenValidConfigName() {
        AWSAssumeRoleCredentialsProvider testProvider = new AWSAssumeRoleCredentialsProvider();

        testProvider.configure(testConfigs);

        assertEquals("test-external-id", testProvider.getOptionalField(testConfigs, AWSAssumeRoleCredentialsProvider.EXTERNAL_ID_CONFIG));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRequiredFieldThrowsExceptionGivenNonExistentConfigName() {
        AWSAssumeRoleCredentialsProvider testProvider = new AWSAssumeRoleCredentialsProvider();

        testProvider.configure(testConfigs);

        assertNull(testProvider.getRequiredField(testConfigs, NON_EXISTENT_FIELD_NAME));
    }

    @Test
    public void testGetRequiredFieldReturnsValueGivenValidConfigName() {
        AWSAssumeRoleCredentialsProvider testProvider = new AWSAssumeRoleCredentialsProvider();

        testProvider.configure(testConfigs);

        assertEquals("test-external-id", testProvider.getRequiredField(testConfigs, AWSAssumeRoleCredentialsProvider.EXTERNAL_ID_CONFIG));
    }
}
