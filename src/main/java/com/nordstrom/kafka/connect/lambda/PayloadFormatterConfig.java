package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import com.nordstrom.kafka.connect.formatters.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.lang.reflect.InvocationTargetException;

public class PayloadFormatterConfig extends AbstractConfig {
    static final String CONFIG_GROUP_NAME = "Payload format";

    static final String FORMATTER_CLASS_KEY = "payload.formatter.class";
    static final String FORMATTER_CLASS_DOC = "Implementation class that formats the invocation payload";
    static final String FORMATTER_PREFIX = "payload.formatter.";
    static final String KEY_SCHEMA_VISIBILITY_KEY = FORMATTER_PREFIX + "key.schema.visibility";
    static final String KEY_SCHEMA_VISIBILITY_DOC = "Determines visibility of the key schema (none, min, all)";
    static final String VALUE_SCHEMA_VISIBILITY_KEY = FORMATTER_PREFIX + "value.schema.visibility";
    static final String VALUE_SCHEMA_VISIBILITY_DOC = "Determines visibility of the value schema (none, min, all)";

    static final String SCHEMA_VISIBILITY_DEFAULT = "min";
    static final List<String> SCHEMA_VISIBILITY_LIST = Arrays.asList("none", "min", "all");

    PayloadFormatterConfig(final Map<String, String> parsedConfig) {
        super(configDef(), parsedConfig);
    }

    @SuppressWarnings("unchecked")
    public PayloadFormatter getPayloadFormatter() {
        try {
            PayloadFormatter payloadFormatter = ((Class<? extends PayloadFormatter>)
                getClass(FORMATTER_CLASS_KEY)).getDeclaredConstructor().newInstance();

            if (payloadFormatter instanceof Configurable) {
                Map<String, Object>configs = originalsWithPrefix(FORMATTER_PREFIX);
                ((Configurable)payloadFormatter).configure(configs);
            }
            return payloadFormatter;

        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            throw new ConnectException("Unable to create " + FORMATTER_CLASS_KEY, e);
        }
    }

    public static ConfigDef configDef() {
        return configDef(new ConfigDef());
    }

    public static ConfigDef configDef(ConfigDef base) {
        int orderInGroup = 0;

        return new ConfigDef(base)
            .define(FORMATTER_CLASS_KEY,
                ConfigDef.Type.CLASS,
                PlainPayloadFormatter.class,
                new FormatterClassValidator(),
                ConfigDef.Importance.LOW,
                FORMATTER_CLASS_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Invocation payload formatter class")

            .define(KEY_SCHEMA_VISIBILITY_KEY,
                ConfigDef.Type.STRING,
                SCHEMA_VISIBILITY_DEFAULT,
                new SchemaVisibilityValidator(),
                ConfigDef.Importance.LOW,
                KEY_SCHEMA_VISIBILITY_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Key schema visibility",
                new SchemaVisibilityRecommender())

            .define(VALUE_SCHEMA_VISIBILITY_KEY,
                ConfigDef.Type.STRING,
                SCHEMA_VISIBILITY_DEFAULT,
                new SchemaVisibilityValidator(),
                ConfigDef.Importance.LOW,
                VALUE_SCHEMA_VISIBILITY_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Value schema visibility",
                new SchemaVisibilityRecommender());
    }

    static class FormatterClassValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object formatter) {
          if (formatter instanceof Class && PayloadFormatter.class.isAssignableFrom((Class<?>)formatter)) {
              return;
          }

          throw new ConfigException(name, formatter, "Class must extend: " + PayloadFormatter.class);
        }

        @Override
        public String toString() {
            return "Any class implementing: " + PayloadFormatter.class;
        }
    }

    static class SchemaVisibilityValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object visibility) {
            if (SCHEMA_VISIBILITY_LIST.contains(visibility)) {
                return;
            }

            throw new ConfigException(name, visibility, "Must be one of " + SCHEMA_VISIBILITY_LIST);
        }
    }

    static class SchemaVisibilityRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.asList(SCHEMA_VISIBILITY_LIST);
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }
}
