package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.RequestTooLargeException;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;
import static org.junit.Assert.*;

public class InvocationClientTest {
    @Test
    public void testBuilderDefaults() {
        InvocationClient.Builder builder = new InvocationClient.Builder();
        assertNull(builder.getFunctionArn());
        assertNull(builder.getRegion());
        assertEquals(InvocationMode.SYNC, builder.getInvocationMode());
        assertEquals(InvocationFailure.STOP, builder.getFailureMode());
        assertEquals(Duration.ofMinutes(5), builder.getInvocationTimeout());
        assertNull(builder.getClientConfiguration());
        assertNull(builder.getCredentialsProvider());
    }

    @Test
    public void testBuilderReflexiveProperties() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        InvocationClient.Builder builder = new InvocationClient.Builder()
            .setFunctionArn("test-function-arn")
            .setRegion("us-test-region")
            .setInvocationMode(InvocationMode.ASYNC)
            .setFailureMode(InvocationFailure.DROP)
            .setInvocationTimeout(Duration.ofSeconds(123))
            .withClientConfiguration(clientConfiguration)
            .withCredentialsProvider(credentialsProvider);

        assertEquals("test-function-arn", builder.getFunctionArn());
        assertEquals("us-test-region", builder.getRegion());
        assertEquals(InvocationMode.ASYNC, builder.getInvocationMode());
        assertEquals(InvocationFailure.DROP, builder.getFailureMode());
        assertEquals(Duration.ofSeconds(123), builder.getInvocationTimeout());
        assertSame(clientConfiguration, builder.getClientConfiguration());
        assertSame(credentialsProvider, builder.getCredentialsProvider());
    }

    @Test(expected = IllegalStateException.class)
    public void ensureFunctionArnIsRequired() {
        InvocationClient.Builder builder = new InvocationClient.Builder()
            //.setFunctionArn("no-function-arn")
            .setRegion("us-test-region");

        builder.build();
    }

    @Test(expected = RequestTooLargeException.class)
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeStopThrowsException() {
        InvocationClient client = newClientWithFailureMode(InvocationFailure.STOP);

        client.checkPayloadSizeForInvocationType(
            "testpayload".getBytes(),
            InvocationType.RequestResponse,
            Instant.now(),
            new RequestTooLargeException("Request payload is too large!"));
    }

    @Test
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeDropContinues() {
        InvocationResponse testResp = null;
        RequestTooLargeException ex = null;

        InvocationClient client = newClientWithFailureMode(InvocationFailure.DROP);

        try {
            testResp = client.checkPayloadSizeForInvocationType(
                "testpayload".getBytes(),
                InvocationType.RequestResponse,
                Instant.now(),
                new RequestTooLargeException("Request payload is too large!"));
        } catch (RequestTooLargeException e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(testResp);
        assertEquals(413, testResp.getStatusCode().intValue());
    }

    InvocationClient newClientWithFailureMode(InvocationFailure failureMode) {
        return new InvocationClient.Builder()
            .setFunctionArn("test-function")
            .setRegion("test-region-1")
            .setFailureMode(failureMode)
            .build();
    }
}
