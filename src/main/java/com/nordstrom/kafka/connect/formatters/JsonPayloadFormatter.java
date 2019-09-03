package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonConverter converter = new JsonConverter();
  private final JsonDeserializer deserializer = new JsonDeserializer();
  private boolean includeSchema = true;

  public JsonPayloadFormatter() {
    converter.configure(emptyMap(), false);
    deserializer.configure(emptyMap(), false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    final Object schemasEnable = configs.get("formatter.schemas.enable");
    if (schemasEnable != null) {
      includeSchema = Boolean.parseBoolean(schemasEnable.toString());
    }
  }

  public String format(final SinkRecord record) {
    try {
      if (includeSchema) {
        // This is ugly as we need to handle all variations of key/value schema not present.
        if (record.keySchema() != null && record.valueSchema() != null) {
          Payload<JsonNode, JsonNode> payload = new Payload<>(record);
          payload.setKey(deserializeRecordKey(record));
          payload.setValue(deserializeRecordValue(record));
          return mapper.writeValueAsString(payload);
        } else if (record.keySchema() == null && record.valueSchema() != null) {
          Payload<Object, JsonNode> payload = new Payload<>(record);
          payload.setKey(record.key());
          payload.setValue(deserializeRecordValue(record));
          return mapper.writeValueAsString(payload);
        } else if (record.keySchema() != null && record.valueSchema() == null) {
          Payload<JsonNode, Object> payload = new Payload<>(record);
          payload.setKey(deserializeRecordKey(record));
          payload.setValue(record.value());
          return mapper.writeValueAsString(payload);
        } else if (record.keySchema() == null && record.valueSchema() == null) {
          Payload<Object, Object> payload = new Payload<>(record);
          payload.setKey(record.key());
          payload.setValue(record.value());
          return mapper.writeValueAsString(payload);
        }
      } else {
        // Disable schema serialization in converter.
        Map<String, String> configs = new HashMap<>();
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        converter.configure(configs, false);

        Payload<Object, Object> payload = new Payload<>(record);
        payload.setKey(deserializeRecordKey(record));
        payload.setValue(deserializeRecordValue(record));
        //Do not include schema name/version if present.
        payload.setKeySchemaName(null);
        payload.setKeySchemaVersion(null);
        payload.setValueSchemaName(null);
        payload.setValueSchemaVersion(null);
        return mapper.writeValueAsString(payload);
      }
    } catch (JsonProcessingException e) {
      throw new PayloadFormattingException(e);
    }

    return null;
  }

  public String formatBatch(final Collection<SinkRecord> records) {
    return "[{\"hello\": \"world\"}]";
  }

  private JsonNode deserializeRecordKey(final SinkRecord record) {
    return deserialize(record.topic(), record.keySchema(), record.key());
  }

  private JsonNode deserializeRecordValue(final SinkRecord record) {
    return deserialize(record.topic(), record.valueSchema(), record.value());
  }

  private JsonNode deserialize(final String topic, final Schema schema, final Object value) {
    return deserializer.deserialize(topic, converter.fromConnectData(topic, schema, value));
  }

}
