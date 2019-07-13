package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.junit.Assert.*;

public class AwsLambdaUtilTest {

    @Test(expected = RequestTooLargeException.class)
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeStopThrowsException() {

        final Configuration testOptConfigs = new Configuration("test-profile", "testhost", 123, "test-region", InvocationFailure.STOP, "test-arn", "test-session", "test-external-id");
        final AwsLambdaUtil testUtil = new AwsLambdaUtil(testOptConfigs, new HashMap<>());

        testUtil.checkPayloadSizeForInvocationType("testpayload".getBytes(), InvocationType.RequestResponse, Instant.now(), new RequestTooLargeException("Request payload is too large!"));
    }

    @Test
    public void testCheckPayloadSizeForInvocationTypeWithInvocationFailureModeDropContinues() {

        AwsLambdaUtil.InvocationResponse testResp = null;
        RequestTooLargeException ex = null;

        final Configuration testOptConfigs = new Configuration("test-profile", "testhost", 123, "test-region", InvocationFailure.DROP, "test-arn", "test-session", "test-external-id");
        final AwsLambdaUtil testUtil = new AwsLambdaUtil(testOptConfigs, new HashMap<>());

        try {
            testResp = testUtil.checkPayloadSizeForInvocationType("testpayload".getBytes(), InvocationType.RequestResponse, Instant.now(), new RequestTooLargeException("Request payload is too large!"));
        } catch (RequestTooLargeException e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(testResp);
        assertEquals(413, testResp.getStatusCode().intValue());
    }
}
