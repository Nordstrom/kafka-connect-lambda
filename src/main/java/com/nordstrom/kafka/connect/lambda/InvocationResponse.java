package com.nordstrom.kafka.connect.lambda;

import java.time.Instant;

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
