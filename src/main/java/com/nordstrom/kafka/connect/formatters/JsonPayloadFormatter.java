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
  enum SchemaVisibility {
    ALL,
    MIN,
    NONE
  }

  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonConverter converter = new JsonConverter();
  private final JsonConverter converterSansSchema = new JsonConverter();
  private final JsonDeserializer deserializer = new JsonDeserializer();
  private SchemaVisibility keyVisibility = SchemaVisibility.MIN;
  private SchemaVisibility valueVisibility = SchemaVisibility.MIN;

  public JsonPayloadFormatter() {
    converter.configure(emptyMap(), false);

    Map<String, String> configs = new HashMap<>();
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
    converterSansSchema.configure(configs, false);

    deserializer.configure(emptyMap(), false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    keyVisibility = configureVisibility(configs, "formatter.key.schema.visibility");
    valueVisibility = configureVisibility(configs, "formatter.value.schema.visibility");
  }

  private SchemaVisibility configureVisibility(final Map<String, ?> configs, final String key) {
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

  public String format(final SinkRecord record) {
    try {
      Object deserializedKey;
      Object deserializedValue;
      if (record.keySchema() == null) {
        deserializedKey = record.key();
      } else {
        deserializedKey = deserialize(keyVisibility, record.topic(), record.keySchema(), record.key());
      }
      if (record.valueSchema() == null) {
        deserializedValue = record.value();
      } else {
        deserializedValue = deserialize(valueVisibility, record.topic(), record.valueSchema(), record.value());
      }

      Payload<Object, Object> p = new Payload<>(record);
      p.setKey(deserializedKey);
      p.setValue(deserializedValue);
      if (keyVisibility == SchemaVisibility.NONE) {
        p.setKeySchemaName(null);
        p.setKeySchemaVersion(null);
      }
      if (valueVisibility == SchemaVisibility.NONE) {
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


  private JsonNode deserialize(final SchemaVisibility visibility, final String topic, final Schema schema, final Object value) {
    if (visibility == SchemaVisibility.ALL) {
      return deserializer.deserialize(topic, converter.fromConnectData(topic, schema, value));
    } else {
      return deserializer.deserialize(topic, converterSansSchema.fromConnectData(topic, schema, value));
    }
  }

}
