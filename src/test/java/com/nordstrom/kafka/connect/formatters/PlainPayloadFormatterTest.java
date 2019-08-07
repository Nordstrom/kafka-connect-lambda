package com.nordstrom.kafka.connect.formatters;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.SchemaBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordstrom.kafka.connect.formatters.PlainPayload;

public class PlainPayloadFormatterTest {

    private Schema keySchema;
    private Schema valueSchema;

    @Before
    public void setup() {
        keySchema = SchemaBuilder.struct()
            .name("com.test.HelloKey")
            .field("name", Schema.STRING_SCHEMA)
            .build();
        valueSchema = SchemaBuilder.struct()
            .name("com.test.HelloValue")
            .field("name", Schema.STRING_SCHEMA)
            .build();
    }

    @Test
    public void testFormatSingleRecord() throws IOException {
        PlainPayloadFormatter f = new PlainPayloadFormatter();
        SinkRecord record = new SinkRecord("test-topic", 1, keySchema, "test-key", valueSchema, "test-value", 2, 3L, TimestampType.NO_TIMESTAMP_TYPE);

        String result = f.format(record);

        PlainPayload payload = new ObjectMapper()
            .readValue(result, PlainPayload.class);

        assertEquals("test-topic", payload.getTopic());
        assertEquals(1, payload.getPartition());
        assertEquals("test-key", payload.getKey());
        assertEquals("com.test.HelloKey", payload.getKeySchemaName());
        assertEquals("test-value", payload.getValue());
        assertEquals("com.test.HelloValue", payload.getValueSchemaName());
        assertEquals(2, payload.getOffset());
        assertEquals(new Long(3), payload.getTimestamp());
        assertEquals("NoTimestampType", payload.getTimestampTypeName());
    }

    @Test
    public void testFormatBatchOfRecords() throws IOException {
        PlainPayloadFormatter f = new PlainPayloadFormatter();

        List<SinkRecord> records = Arrays.asList(
            new SinkRecord("test-topic", 1, null, "test-key1", null, "test-value1", 0),
            new SinkRecord("test-topic", 1, null, "test-key2", null, "test-value2", 1),
            new SinkRecord("test-topic", 1, null, "test-key3", null, "test-value3", 2)
        );

        String result = f.formatBatch(records);

        PlainPayload[] payloads = new ObjectMapper()
            .readValue(result, PlainPayload[].class);

        assertEquals(3, payloads.length);
        for (int i = 0; i < payloads.length; i++) {
            assertEquals(i, payloads[i].getOffset());
        }
    }
}
