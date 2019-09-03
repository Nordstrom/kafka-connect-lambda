package com.nordstrom.kafka.connect.lambda;

import com.nordstrom.kafka.connect.formatters.JsonPayloadFormatter;
import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.nordstrom.kafka.connect.formatters.PlainPayloadFormatter;

import static org.junit.Assert.*;

import java.util.HashMap;

public class LambdaSinkConnectorConfigTest {
    @Test
    public void minimalConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
            {
                put("aws.lambda.function.arn", "my-function");
            }
        });

        assertTrue("Expected auto-generated connector name",
            config.getConnectorName().contains("LambdaSinkConnector-Unnamed"));

        assertEquals("my-function", config.getAwsFunctionArn());

        assertNotNull(config.getAwsRegion());
        assertNotNull(config.getAwsClientConfiguration());
        assertNotNull(config.getInvocationTimeout());
        assertNotNull(config.getFailureMode());
        assertNotNull(config.getInvocationMode());
        assertNotNull(config.getRetriableErrorCodes());
        assertNotNull(config.getIamRoleArn());
        assertNotNull(config.getIamExternalId());
        assertNotNull(config.getIamSessionName());

        assertEquals(DefaultAWSCredentialsProviderChain.class, config.getAwsCredentialsProvider().getClass());
        assertEquals(PlainPayloadFormatter.class, config.getPayloadFormatter().getClass());
        assertTrue(config.isBatchingEnabled());
        assertTrue(config.isPayloadFormatterSchemasEnable());
    }

    @Test
    public void sampleConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
            {
                put("name", "test-connector");
                put("aws.region", "test-region");
                put("aws.lambda.function.arn", "test-function");
                put("aws.lambda.invocation.timeout.ms", "123");
                put("aws.lambda.invocation.mode", "SYNC");
                put("aws.lambda.invocation.failure.mode", "DROP");
                put("aws.lambda.batch.enabled", "true");
                put("retriable.error.codes", "1,2,3");
                put("retry.backoff.millis", "123");
                put("retries.max", "123");
            }
        });

        assertEquals("test-connector", config.getConnectorName());
        assertEquals("test-region", config.getAwsRegion());
        assertEquals("PT0.123S", config.getInvocationTimeout().toString());
        assertEquals(InvocationMode.SYNC, config.getInvocationMode());
        assertEquals(InvocationFailure.DROP, config.getFailureMode());
        assertTrue(config.isBatchingEnabled());
        assertEquals(3, config.getRetriableErrorCodes().size());
        assertEquals(123, config.getRetryBackoffTimeMillis());
        assertEquals(123, config.getRetries());
    }

    @Test
    public void jsonPayloadFormatterConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
                {
                    put("name", "test-connector");
                    put("aws.lambda.function.arn", "test-function");
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                }
            });

        assertEquals("test-connector", config.getConnectorName());
        assertTrue(config.isBatchingEnabled());
        assertEquals(JsonPayloadFormatter.class, config.getPayloadFormatter().getClass());
        assertTrue(config.isPayloadFormatterSchemasEnable());
    }

    @Test
    public void jsonPayloadFormatterNoSchemasConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
                {
                    put("name", "test-connector");
                    put("aws.lambda.function.arn", "test-function");
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                    put("payload.formatter.schemas.enable", "false");
                }
            });

        assertEquals("test-connector", config.getConnectorName());
        assertTrue(config.isBatchingEnabled());
        assertEquals(JsonPayloadFormatter.class, config.getPayloadFormatter().getClass());
        assertFalse(config.isPayloadFormatterSchemasEnable());
    }

}
