package com.nordstrom.kafka.connect.lambda;

import com.nordstrom.kafka.connect.formatters.*;

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;

public class PayloadFormatterConfigTest {
    @Test
    public void defaultPayloadFormatterConfig() {
        PayloadFormatterConfig config = new PayloadFormatterConfig(
            new HashMap<String, String>() {
                {
                }
            });

        assertEquals(PlainPayloadFormatter.class, config.getPayloadFormatter().getClass());
    }

    @Test
    public void jsonPayloadFormatterConfig() {
        PayloadFormatterConfig config = new PayloadFormatterConfig(
            new HashMap<String, String>() {
                {
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                }
            });

        PayloadFormatter formatter = config.getPayloadFormatter();
        assertEquals(JsonPayloadFormatter.class, formatter.getClass());
        assertEquals(SchemaVisibility.MIN, ((JsonPayloadFormatter)formatter).getKeySchemaVisibility());
        assertEquals(SchemaVisibility.MIN, ((JsonPayloadFormatter)formatter).getValueSchemaVisibility());
    }

    @Test
    public void jsonPayloadFormatterKeySchemaVisibilityConfig() {
        PayloadFormatterConfig config = new PayloadFormatterConfig(
            new HashMap<String, String>() {
                {
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                    put("payload.formatter.key.schema.visibility", "none");
                }
            });

        PayloadFormatter formatter = config.getPayloadFormatter();
        assertEquals(SchemaVisibility.NONE, ((JsonPayloadFormatter)formatter).getKeySchemaVisibility());
        assertEquals(SchemaVisibility.MIN, ((JsonPayloadFormatter)formatter).getValueSchemaVisibility());
    }

    @Test
    public void jsonPayloadFormatterValueSchemaVisibilityConfig() {
        PayloadFormatterConfig config = new PayloadFormatterConfig(
            new HashMap<String, String>() {
                {
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                    put("payload.formatter.value.schema.visibility", "none");
                }
            });

        PayloadFormatter formatter = config.getPayloadFormatter();
        assertEquals(SchemaVisibility.MIN, ((JsonPayloadFormatter)formatter).getKeySchemaVisibility());
        assertEquals(SchemaVisibility.NONE, ((JsonPayloadFormatter)formatter).getValueSchemaVisibility());
    }

    @Test(expected = ConfigException.class)
    public void jsonPayloadFormatterSchemaVisibilityConfigValidatorThrowsException() {
        PayloadFormatterConfig config = new PayloadFormatterConfig(
            new HashMap<String, String>() {
                {
                    put("payload.formatter.class", JsonPayloadFormatter.class.getCanonicalName());
                    put("payload.formatter.key.schema.visibility", "x-none");
                }
            });
    }
}
