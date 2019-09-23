package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;

public class LambdaSinkConnectorConfigTest {
    @Test
    public void minimalConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
            {
                put("aws.lambda.function.arn", "test-function");
            }
        });

        assertTrue("Expected auto-generated connector name",
            config.getConnectorName().contains("LambdaSinkConnector-Unnamed"));

        assertNotNull(config.getInvocationClientConfig());
        assertNotNull(config.getPayloadFormatterConfig());
        assertTrue(config.isBatchingEnabled());
        assertNotNull(config.getRetriableErrorCodes());
        assertEquals(500L, config.getRetryBackoffTimeMillis());
        assertEquals(5, config.getRetries());
    }

    @Test
    public void sampleConfig() {
        LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(
            new HashMap<String, String>() {
            {
                put("name", "test-connector");
                put("aws.region", "us-test-region");
                put("aws.lambda.function.arn", "test-function");
                put("aws.lambda.invocation.timeout.ms", "123");
                put("aws.lambda.invocation.mode", "SYNC");
                put("aws.lambda.invocation.failure.mode", "DROP");
                put("aws.lambda.batch.enabled", "true");
                put("retriable.error.codes", "1,2,3");
                put("retry.backoff.millis", "123");
                put("retries.max", "456");
            }
        });

        assertEquals("test-connector", config.getConnectorName());
        assertNotNull(config.getInvocationClientConfig());
        assertNotNull(config.getPayloadFormatterConfig());
        assertTrue(config.isBatchingEnabled());
        assertEquals(3, config.getRetriableErrorCodes().size());
        assertEquals(123, config.getRetryBackoffTimeMillis());
        assertEquals(456, config.getRetries());
    }
}
