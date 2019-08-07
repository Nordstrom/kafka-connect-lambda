package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import com.nordstrom.kafka.connect.utils.Guard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwsLambdaUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsLambdaUtil.class);

    private static final int MEGABYTE_SIZE = 1024 * 1024;
    private static final int KILOBYTE_SIZE = 1024;

    private static final int maxSyncPayloadSizeBytes = (6 * MEGABYTE_SIZE);
    private static final int maxAsyncPayloadSizeBytes = (256 * KILOBYTE_SIZE);

    private final AWSLambdaAsync lambdaClient;
    private final InvocationFailure failureMode;

    public AwsLambdaUtil(ClientConfiguration clientConfiguration,
        AWSCredentialsProvider credentialsProvider,
        InvocationFailure failureMode) {

        Guard.verifyNotNull(clientConfiguration, "clientConfiguration");
        Guard.verifyNotNull(credentialsProvider, "credentialsProvider");

        final AWSLambdaAsyncClientBuilder builder = AWSLambdaAsyncClientBuilder.standard()
            .withClientConfiguration(clientConfiguration)
            .withCredentials(credentialsProvider);

        this.failureMode = failureMode;
        this.lambdaClient = builder.build();

        LOGGER.debug("AWS Lambda client initialized");
    }

    public InvocationResponse invokeSync(
            final String functionName,
            final byte[] payload,
            final Duration timeout) {
        return this.invoke(functionName, payload, timeout, InvocationType.RequestResponse);
    }

    public InvocationResponse invokeAsync(
            final String functionName,
            final byte[] payload,
            final Duration timeout) {
        return this.invoke(functionName, payload, timeout, InvocationType.Event);
    }

    InvocationResponse invoke(
            final String functionName,
            final byte[] payload,
            final Duration timeout,
            final InvocationType event
    ) {
        final InvokeRequest request = new InvokeRequest()
                .withInvocationType(event)
                .withFunctionName(functionName)
                .withPayload(ByteBuffer.wrap(payload));

        final Future<InvokeResult> futureResult = this.lambdaClient.invokeAsync(request);

        final Instant start = Instant.now();
        try {
            final InvokeResult result = futureResult.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return new InvocationResponse(result.getStatusCode(), result.getLogResult(),
                    result.getFunctionError(), start, Instant.now());
        } catch (RequestTooLargeException e) {
            return checkPayloadSizeForInvocationType(payload, event, start, e);
        } catch (final InterruptedException | ExecutionException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new LambdaInvocationException(e);
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

    private class LambdaInvocationException extends RuntimeException {
        public LambdaInvocationException(final Throwable e) {
            super(e);
        }
    }

    public class InvocationResponse {

        private final String errorString;
        private final String responseString;
        private final Integer statusCode;
        private final Instant start;
        private final Instant end;

        public InvocationResponse(
                final Integer statusCode,
                final String logResult,
                final String functionError,
                final Instant start,
                final Instant end) {

            this.statusCode = statusCode;
            this.responseString = logResult;
            this.errorString = functionError;
            this.start = start;
            this.end = end;

        }

        public Instant getStart() {
            return this.start;
        }

        public Instant getEnd() {
            return this.end;
        }

        public String getErrorString() {
            return this.errorString;
        }

        public String getResponseString() {
            return this.responseString;
        }

        public Integer getStatusCode() {
            return this.statusCode;
        }

    }
}
