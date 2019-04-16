package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class AwsLambdaUtilTest {

    @Test(expected = RequestTooLargeException.class)
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeStopThrowsException() {

        final Configuration testConfiguration = new Configuration("test-profile", "testhost", 123, "test-region", InvocationFailure.STOP);
        final AwsLambdaUtil testUtil = new AwsLambdaUtil(testConfiguration);

        testUtil.checkPayloadSizeForInvocationType("testpayload".getBytes(), InvocationType.RequestResponse, Instant.now(), new RequestTooLargeException("Request payload is too large!"));
    }

    @Test
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeDropContinues() {

        AwsLambdaUtil.InvocationResponse testResp = null;
        RequestTooLargeException ex = null;

        final Configuration testConfiguration = new Configuration("test-profile", "testhost", 123, "test-region", InvocationFailure.DROP);
        final AwsLambdaUtil testUtil = new AwsLambdaUtil(testConfiguration);

        try {
            testResp = testUtil.checkPayloadSizeForInvocationType("testpayload".getBytes(), InvocationType.RequestResponse, Instant.now(), new RequestTooLargeException("Request payload is too large!"));
        } catch (RequestTooLargeException e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(testResp);
        assertEquals(400, testResp.getStatusCode().intValue());
    }
}
