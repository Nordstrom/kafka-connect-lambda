package com.nordstrom.kafka.connect.formatters;

public class PayloadFormattingException extends RuntimeException {
    public PayloadFormattingException (final Throwable e) {
        super(e);
    }
}
