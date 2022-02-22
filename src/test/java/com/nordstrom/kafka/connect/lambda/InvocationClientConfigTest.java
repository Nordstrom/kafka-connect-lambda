package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.nordstrom.kafka.connect.auth.AWSUserCredentialsProvider;

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
        assertNull(builder.getEndpoint());
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
                put("aws.credentials.provider.class", AWSUserCredentialsProvider.class.getName());
                put("aws.credentials.provider.access.key", "test-access-key");
                put("aws.credentials.provider.secret.key", "test-secret-key");
            }
        });

        assertEquals("test-function", builder.getFunctionArn());
        assertEquals("us-test-region", builder.getRegion());
        assertEquals(123, builder.getInvocationTimeout().toMillis());
        assertEquals(InvocationMode.SYNC, builder.getInvocationMode());
        assertEquals(InvocationFailure.DROP, builder.getFailureMode());
        assertNull(builder.getEndpoint());

        assertEquals(AWSUserCredentialsProvider.class, builder.getCredentialsProvider().getClass());
        AWSUserCredentialsProvider credentialsProvider = (AWSUserCredentialsProvider)builder.getCredentialsProvider();
        assertEquals("test-access-key", credentialsProvider.getAccessKey());
        assertEquals("test-secret-key", credentialsProvider.getSecretKey());
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
