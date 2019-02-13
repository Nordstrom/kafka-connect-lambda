package com.nordstrom.kafka.connect.lambda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.List;

public class JsonUtil {

    static final ObjectWriter stringListWriter = new ObjectMapper()
            .writerFor(new TypeReference<List<String>>() {
            });

    public static String jsonify(final List<String> stringRecords) {
        try {
            return stringListWriter.writeValueAsString(stringRecords);
        } catch (final JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }
}
