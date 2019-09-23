package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class JsonPayloadFormatter implements PayloadFormatter, Configurable {
  private final ObjectWriter recordWriter = new ObjectMapper().writerFor(Payload.class);
  private final ObjectWriter recordsWriter = new ObjectMapper().writerFor(Payload[].class);
  private final JsonConverter converter = new JsonConverter();
  private final JsonConverter converterSansSchema = new JsonConverter();
  private final JsonDeserializer deserializer = new JsonDeserializer();
  private SchemaVisibility keySchemaVisibility = SchemaVisibility.MIN;
  private SchemaVisibility valueSchemaVisibility = SchemaVisibility.MIN;

  public JsonPayloadFormatter() {
    converter.configure(emptyMap(), false);

    Map<String, String> configs = new HashMap<>();
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    converterSansSchema.configure(configs, false);

    deserializer.configure(emptyMap(), false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    keySchemaVisibility = configureSchemaVisibility(configs, "key.schema.visibility");
    valueSchemaVisibility = configureSchemaVisibility(configs, "value.schema.visibility");
  }

  private SchemaVisibility configureSchemaVisibility(final Map<String, ?> configs, final String key) {
    SchemaVisibility viz = SchemaVisibility.MIN;
    final Object visibility = configs.get(key);
    if (visibility != null) {
      switch (visibility.toString()) {
        case "all":
          viz = SchemaVisibility.ALL;
          break;
        case "min":
          viz = SchemaVisibility.MIN;
          break;
        case "none":
          viz = SchemaVisibility.NONE;
          break;
      }
    }

    return viz;
  }

  public SchemaVisibility getKeySchemaVisibility() {
    return keySchemaVisibility;
  }

  public SchemaVisibility getValueSchemaVisibility() {
    return valueSchemaVisibility;
  }

  public String format(final SinkRecord record) {
    try {
      return recordWriter.writeValueAsString(recordToPayload(record));
    } catch (JsonProcessingException e) {
      throw new PayloadFormattingException(e);
    }
  }

  public String format(final Collection<SinkRecord> records) {
    final Payload[] payloads = records
        .stream()
        .map(this::recordToPayload)
        .toArray(Payload[]::new);

    try {
      return recordsWriter.writeValueAsString(payloads);
    } catch (final JsonProcessingException e) {
      throw new PayloadFormattingException(e);
    }
  }

  private Payload<Object, Object> recordToPayload(final SinkRecord record) {
    Object deserializedKey;
    Object deserializedValue;
    if (record.keySchema() == null) {
      deserializedKey = record.key();
    } else {
      deserializedKey = deserialize(keySchemaVisibility, record.topic(), record.keySchema(), record.key());
    }
    if (record.valueSchema() == null) {
      deserializedValue = record.value();
    } else {
      deserializedValue = deserialize(valueSchemaVisibility, record.topic(), record.valueSchema(), record.value());
    }

    Payload<Object, Object> payload = new Payload<>(record);
    payload.setKey(deserializedKey);
    payload.setValue(deserializedValue);
    if (keySchemaVisibility == SchemaVisibility.NONE) {
      payload.setKeySchemaName(null);
      payload.setKeySchemaVersion(null);
    }
    if (valueSchemaVisibility == SchemaVisibility.NONE) {
      payload.setValueSchemaName(null);
      payload.setValueSchemaVersion(null);
    }

    return payload;
  }

  private JsonNode deserialize(final SchemaVisibility schemaVisibility, final String topic, final Schema schema, final Object value) {
    if (schemaVisibility == SchemaVisibility.ALL) {
      return deserializer.deserialize(topic, converter.fromConnectData(topic, schema, value));
    } else {
      return deserializer.deserialize(topic, converterSansSchema.fromConnectData(topic, schema, value));
    }
  }

}
