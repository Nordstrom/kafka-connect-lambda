package com.nordstrom.kafka.connect.lambda;

/**
 * Facilitates failure behavior to be configurable: stop or drop and continue
 */
public enum InvocationFailure {
    STOP, DROP
}
