package com.nordstrom.kafka.connect.lambda;

public class KafkaSerializationException extends RuntimeException {

    public KafkaSerializationException(final Throwable e) {
        super(e);
    }
}
