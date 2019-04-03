package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
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

    private final AWSLambdaAsync lambdaClient;

    public AwsLambdaUtil(final Configuration config) {
        Guard.verifyNotNull(config, "config");
        final AWSLambdaAsyncClientBuilder builder = AWSLambdaAsyncClientBuilder.standard();

        // Will check if there's proxy configuration in the environment; if
        // there's any will construct the client with it.
        if (config.getHttpProxyHost().isPresent()) {
            final ClientConfiguration clientConfiguration = new ClientConfiguration()
                    .withProxyHost(config.getHttpProxyHost().get());
            if (config.getHttpProxyPort().isPresent()) {
                clientConfiguration.setProxyPort(config.getHttpProxyPort().get());
            }
            builder.setClientConfiguration(clientConfiguration);
            LOGGER.info("Setting proxy configuration for AWS Lambda Async client host: {} port {}",
                    config.getHttpProxyHost().get(), config.getHttpProxyPort().orElse(-1));
        }

        // If there's a credentials profile configuration in the environment will
        // use it.
        if (config.getCredentialsProfile().isPresent()) {
            builder.setCredentials(new ProfileCredentialsProvider(config.getCredentialsProfile().get()));
            LOGGER.info("Using aws credentials profile {} for AWS Lambda client",
                    config.getCredentialsProfile().get());
        }

        if (config.getAwsRegion().isPresent()) {
            builder.setRegion(config.getAwsRegion().get());
            LOGGER.info("Using aws region: {}", config.getAwsRegion().toString());
        }

        this.lambdaClient = builder.build();
        LOGGER.info("AWS Lambda client initialized");
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
        } catch (final InterruptedException | ExecutionException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new LambdaInvocationException(e);
        } catch (final TimeoutException e) {
            return new InvocationResponse(504, e.getLocalizedMessage(), e.getLocalizedMessage(), start,
                    Instant.now());
        }
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
