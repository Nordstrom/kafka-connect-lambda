package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.nordstrom.kafka.connect.auth.AWSAssumeRoleCredentialsProvider;

import java.time.Duration;
import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;

public class InvocationClientConfigTest {
    @Test
    public void minimalConfig() {
        InvocationClient.Builder builder = new InvocationClient.Builder();
        new InvocationClientConfig(builder,
            new HashMap<String, String>() {
            {
                put("aws.lambda.function.arn", "test-function");
            }
        });

        assertEquals("test-function", builder.getFunctionArn());
        assertNull(builder.getRegion());
        assertEquals(InvocationMode.SYNC, builder.getInvocationMode());
        assertEquals(InvocationFailure.STOP, builder.getFailureMode());
        assertEquals(Duration.ofMinutes(5), builder.getInvocationTimeout());
        assertNotNull(builder.getClientConfiguration());
        assertEquals(DefaultAWSCredentialsProviderChain.class, builder.getCredentialsProvider().getClass());
    }

    @Test
    public void sampleConfig() {
        InvocationClient.Builder builder = new InvocationClient.Builder();
        new InvocationClientConfig(builder,
            new HashMap<String, String>() {
            {
                put("aws.region", "us-test-region");
                put("aws.lambda.function.arn", "test-function");
                put("aws.lambda.invocation.timeout.ms", "123");
                put("aws.lambda.invocation.mode", "SYNC");
                put("aws.lambda.invocation.failure.mode", "DROP");
                put("aws.lambda.batch.enabled", "true");
                put("aws.credentials.provider.class", AWSAssumeRoleCredentialsProvider.class.getName());
                put("aws.credentials.provider.role.arn", "test-role");
                put("aws.credentials.provider.session.name", "test-session-name");
                put("aws.credentials.provider.external.id", "test-external-id");
            }
        });

        assertEquals("test-function", builder.getFunctionArn());
        assertEquals("us-test-region", builder.getRegion());
        assertEquals(123, builder.getInvocationTimeout().toMillis());
        assertEquals(InvocationMode.SYNC, builder.getInvocationMode());
        assertEquals(InvocationFailure.DROP, builder.getFailureMode());
        assertNotNull(builder.getClientConfiguration());

        assertEquals(AWSAssumeRoleCredentialsProvider.class, builder.getCredentialsProvider().getClass());
        AWSAssumeRoleCredentialsProvider credentialsProvider = (AWSAssumeRoleCredentialsProvider)builder.getCredentialsProvider();
        assertEquals("test-role", credentialsProvider.getRoleArn());
        assertEquals("test-session-name", credentialsProvider.getSessionName());
        assertEquals("test-external-id", credentialsProvider.getExternalId());
    }

    @Test(expected = ConfigException.class)
    public void testInvocationModeValidatorThrowsException() {
        new InvocationClientConfig.InvocationModeValidator()
            .ensureValid("invocation.mode", "foo");
    }

    @Test(expected = ConfigException.class)
    public void testInvocationFailureValidatorThrowsException() {
        new InvocationClientConfig.InvocationFailureValidator()
            .ensureValid("invocation.failure.mode", "foo");
    }
}

