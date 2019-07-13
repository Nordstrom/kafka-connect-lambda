package com.nordstrom.kafka.connect.utils;

/**
 * Guard methods throw an exception should the test fail.
 */
public class Guard {

    public static void verifyNotNullOrEmpty(String string, String argumentName) {
        if (!Facility.isNotNullNorEmpty(string)) {
            throw new IllegalArgumentException(
                    String.format("The argument %1$s should not be null or empty", argumentName));
        }
    }

    public static void verifyNotNull(Object argument, String argumentName) {
        if (!Facility.isNotNull( argument)) {
            throw new IllegalArgumentException(String.format("The argument %1$s should not be null", argumentName));
        }
    }
}
