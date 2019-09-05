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
  class SchemaVisiblityStrategy {
    private boolean all = false;
    private boolean none = false;
    String visibility;

    void configure(Map<String, ?> configs, String key) {
      final Object visibility = configs.get(key);
      if (visibility != null) {
        switch (visibility.toString()) {
          case "all":
            all = true;
            none = false;
            break;
          case "min":
            all = false;
            none = false;
            break;
          case "none":
            all = false;
            none = true;
            break;
        }
        this.visibility = visibility.toString();
      }
    }

    @Override
    public String toString() {
      return "visibility=" + visibility + ", isAll=" + all + ", isNone=" + none;
    }

    public boolean isAll() {
      return all;
    }

    public boolean isNone() {
      return none;
    }
  }

  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonConverter converter = new JsonConverter();
  private final JsonConverter converterSansSchema = new JsonConverter();
  private final JsonDeserializer deserializer = new JsonDeserializer();
  private SchemaVisiblityStrategy keySchemaVisibility = new SchemaVisiblityStrategy();
  private SchemaVisiblityStrategy valueSchemaVisibility = new SchemaVisiblityStrategy();

  public JsonPayloadFormatter() {
    converter.configure(emptyMap(), false);

    Map<String, String> configs = new HashMap<>();
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    converterSansSchema.configure(configs, false);

    deserializer.configure(emptyMap(), false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    keySchemaVisibility.configure(configs, "formatter.key.schema.visibility");
    valueSchemaVisibility.configure(configs, "formatter.value.schema.visibility");
  }

  public String format(final SinkRecord record) {
    try {
      Object deserializedKey;
      Object deserializedValue;
      if (record.keySchema() == null) {
        deserializedKey = record.key();
      } else {
        deserializedKey = deserialize(keySchemaVisibility.isAll(), record.topic(), record.keySchema(), record.key());
      }
      if (record.valueSchema() == null) {
        deserializedValue = record.value();
      } else {
        deserializedValue = deserialize(valueSchemaVisibility.isAll(), record.topic(), record.valueSchema(), record.value());
      }

      Payload<Object, Object> p = new Payload<>(record);
      p.setKey(deserializedKey);
      p.setValue(deserializedValue);
      if (keySchemaVisibility.isNone()) {
        p.setKeySchemaName(null);
        p.setKeySchemaVersion(null);
      }
      if (valueSchemaVisibility.isNone()) {
        p.setValueSchemaName(null);
        p.setValueSchemaVersion(null);
      }

      return mapper.writeValueAsString(p);
    } catch (JsonProcessingException e) {
      throw new PayloadFormattingException(e);
    }
  }

  public String formatBatch(final Collection<SinkRecord> records) {
    return "[{\"hello\": \"world\"}]";
  }


  private JsonNode deserialize(final boolean includeSchema, final String topic, final Schema schema, final Object value) {
    if (includeSchema) {
      return deserializer.deserialize(topic, converter.fromConnectData(topic, schema, value));
    } else {
      return deserializer.deserialize(topic, converterSansSchema.fromConnectData(topic, schema, value));
    }
  }

}
