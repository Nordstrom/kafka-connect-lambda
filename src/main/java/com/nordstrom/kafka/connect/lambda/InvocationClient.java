package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.RequestTooLargeException;

import com.nordstrom.kafka.connect.utils.Facility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InvocationClient {
    public static final InvocationMode DEFAULT_INVOCATION_MODE = InvocationMode.SYNC;
    public static final InvocationFailure DEFAULT_FAILURE_MODE = InvocationFailure.STOP;
    public static final long DEFAULT_INVOCATION_TIMEOUT_MS = 5 * 60 * 1000L;
    private static final Logger LOGGER = LoggerFactory.getLogger(InvocationClient.class);

    private static final int MEGABYTE_SIZE = 1024 * 1024;
    private static final int KILOBYTE_SIZE = 1024;

    private static final int maxSyncPayloadSizeBytes = (6 * MEGABYTE_SIZE);
    private static final int maxAsyncPayloadSizeBytes = (256 * KILOBYTE_SIZE);

    private final AWSLambdaAsync innerClient;
    private final String functionArn;
    private InvocationFailure failureMode;
    private InvocationMode invocationMode;
    private Duration invocationTimeout;

    private InvocationClient(String functionArn, AWSLambdaAsync innerClient) {
        this.functionArn = functionArn;
        this.innerClient = innerClient;
    }

    public InvocationResponse invoke(final byte[] payload) {
        final InvocationType type = invocationMode == InvocationMode.ASYNC
            ? InvocationType.Event : InvocationType.RequestResponse;

        final InvokeRequest request = new InvokeRequest()
                .withInvocationType(type)
                .withFunctionName(functionArn)
                .withPayload(ByteBuffer.wrap(payload));

        final Future<InvokeResult> futureResult = innerClient.invokeAsync(request);

        final Instant start = Instant.now();
        try {
            final InvokeResult result = futureResult.get(invocationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return new InvocationResponse(result.getStatusCode(), result.getLogResult(),
                    result.getFunctionError(), start, Instant.now());
        } catch (RequestTooLargeException e) {
            return checkPayloadSizeForInvocationType(payload, type, start, e);
        } catch (final InterruptedException | ExecutionException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new InvocationException(e);
        } catch (final TimeoutException e) {
            return new InvocationResponse(504, e.getLocalizedMessage(), e.getLocalizedMessage(), start,
                    Instant.now());
        }
    }

    /**
     *
     * @param payload a byte array representation of the payload sent to AWS Lambda service
     * @param event   enumeration type to determine if we are sending in aynch, sync, or no-op mode
     * @param start   time instance when Lambda invocation was started
     * @param e       exception indicative of the payload size being over the max allowable
     * @return        a rolled up Lambda invocation response
     * @throws        RequestTooLargeException is rethrown if the failure mode is set to stop immediately
     */
    InvocationResponse checkPayloadSizeForInvocationType(final byte[] payload, final InvocationType event, final Instant start, final RequestTooLargeException e) {
        switch (event) {

            case Event:
                if (payload.length > maxAsyncPayloadSizeBytes) {
                    LOGGER.error("{} bytes payload exceeded {} bytes invocation limit for asynchronous Lambda call", payload.length, maxAsyncPayloadSizeBytes);
                }
                break;

            case RequestResponse:
                if (payload.length > maxSyncPayloadSizeBytes) {
                    LOGGER.error("{} bytes payload exceeded {} bytes invocation limit for synchronous Lambda call", payload.length, maxSyncPayloadSizeBytes);
                }
                break;

            default:
                LOGGER.info("Dry run call to Lambda with payload size {}", payload.length);
                break;
        }

        if (failureMode.equals(InvocationFailure.STOP)) {
            throw e;
        }
        // Drop message and continue
        return new InvocationResponse(413, e.getLocalizedMessage(), e.getLocalizedMessage(), start, Instant.now());
    }

    private class InvocationException extends RuntimeException {
        public InvocationException(final Throwable e) {
            super(e);
        }
    }

    public static class Builder {
        private String functionArn;
        private InvocationMode invocationMode = DEFAULT_INVOCATION_MODE;
        private InvocationFailure failureMode = DEFAULT_FAILURE_MODE;
        private Duration invocationTimeout = Duration.ofMillis(DEFAULT_INVOCATION_TIMEOUT_MS);

        private final AWSLambdaAsyncClientBuilder innerBuilder;

        public Builder() {
            this.innerBuilder = AWSLambdaAsyncClientBuilder.standard();
        }

        public InvocationClient build() {
            if (functionArn == null || functionArn.isEmpty())
                throw new IllegalStateException("AWS Lambda function ARN cannot be null or empty");

            InvocationClient client = new InvocationClient(functionArn, innerBuilder.build());
            client.failureMode = failureMode;
            client.invocationMode = invocationMode;
            client.invocationTimeout = invocationTimeout;
            return client;
        }

        public String getFunctionArn() {
            return functionArn;
        }

        public Builder setFunctionArn(final String functionArn) {
            this.functionArn = functionArn;
            return this;
        }

        public InvocationFailure getFailureMode() {
            return failureMode;
        }

        public Builder setFailureMode(final InvocationFailure failureMode) {
            this.failureMode = failureMode;
            return this;
        }

        public InvocationMode getInvocationMode() {
            return invocationMode;
        }

        public Builder setInvocationMode(final InvocationMode invocationMode) {
            this.invocationMode = invocationMode;
            return this;
        }

        public Duration getInvocationTimeout() {
            return this.invocationTimeout;
        }

        public Builder setInvocationTimeout(final Duration timeout) {
            this.invocationTimeout = timeout;
            return this;
        }

        public String getRegion() {
            return this.innerBuilder.getRegion();
        }

        public Builder setRegion(final String awsRegion) {
            this.innerBuilder.setRegion(awsRegion);
            return this;
        }

        public ClientConfiguration getClientConfiguration() {
            return this.innerBuilder.getClientConfiguration();
        }

        public Builder withClientConfiguration(final ClientConfiguration clientConfiguration) {
            this.innerBuilder.withClientConfiguration(clientConfiguration);
            return this;
        }

        public AWSCredentialsProvider getCredentialsProvider() {
            return this.innerBuilder.getCredentials();
        }

        public Builder withCredentialsProvider(final AWSCredentialsProvider credentialsProvider) {
            this.innerBuilder.withCredentials(credentialsProvider);
            return this;
        }
    }
}
