package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class AwsLambdaUtilTest {

    @Test(expected = RequestTooLargeException.class)
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeStopThrowsException() {
        AwsLambdaUtil util  = new AwsLambdaUtil(
            new ClientConfiguration(),
            new DefaultAWSCredentialsProviderChain(),
            InvocationFailure.STOP);

        util.checkPayloadSizeForInvocationType(
            "testpayload".getBytes(),
            InvocationType.RequestResponse,
            Instant.now(),
            new RequestTooLargeException("Request payload is too large!"));
    }

    @Test
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeDropContinues() {
        AwsLambdaUtil.InvocationResponse testResp = null;
        RequestTooLargeException ex = null;

        AwsLambdaUtil util  = new AwsLambdaUtil(
            new ClientConfiguration(),
            new DefaultAWSCredentialsProviderChain(),
            InvocationFailure.DROP);

        try {
            testResp = util.checkPayloadSizeForInvocationType(
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
}
