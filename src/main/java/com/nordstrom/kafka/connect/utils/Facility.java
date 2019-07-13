package com.nordstrom.kafka.connect.utils;

/**
 * General utility test methods.
 */
public class Facility {

    public static boolean isNotNullNorEmpty(String string) {
        return string != null && !string.isEmpty();
    }

    public static boolean isNotNull(Object argument) {
        return argument != null;
    }

    public static boolean isInRange(int argument, int beginningInclusive, int endInclusive) {
        if (argument < beginningInclusive || argument > endInclusive) {
            return false;
        }
        return true;
    }

    public static boolean isNotNullAndInRange(Integer argument, int beginningInclusive, int endInclusive) {
        return isNotNull(argument) && isInRange(argument, beginningInclusive, endInclusive);
    }
}
