package com.nordstrom.kafka.connect.lambda;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class InvocationResponse {
    private final String errorString;
    private final ByteBuffer errorDescription;
    private final String responseString;
    private final Integer statusCode;
    private final Instant start;
    private final Instant end;

    public InvocationResponse(
            final Integer statusCode,
            final String logResult,
            final String functionError,
            final ByteBuffer errorDescription,
            final Instant start,
            final Instant end) {

        this.statusCode = statusCode;
        this.responseString = logResult;
        this.errorString = functionError;
        this.errorDescription = errorDescription;
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

    public String getErrorDescription() {
        Charset charset = StandardCharsets.UTF_8;
        return charset.decode(this.errorDescription).toString();
    }

    public String getResponseString() {
        return this.responseString;
    }

    public Integer getStatusCode() {
        return this.statusCode;
    }
}
