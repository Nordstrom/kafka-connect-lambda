package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;

import com.nordstrom.kafka.connect.formatters.PayloadFormatter;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import java.util.concurrent.ThreadLocalRandom;

public class LambdaSinkConnectorConfig extends AbstractConfig {
    static final String CONNECTOR_NAME_KEY = "name";
    static final String CONNECTOR_NAME_DOC = "DEPRECATED Connector name used in logs";

    static final String BATCH_RECORDS_ENABLED_KEY = "aws.lambda.batch.enabled";
    static final String BATCH_RECORDS_ENABLED_DOC = "Determines whether to send individual records, or an array of records, when invoking the Lambda";
    static final boolean BATCH_RECORDS_ENABLED_DEFAULT = true;

    static final String RETRIES_MAX_KEY = "retries.max";
    static final String RETRIES_MAX_DOC = "Max number of times to retry the Lambda invocation";
    static final int RETRIES_DEFAULT = 5;

    static final String RETRY_BACKOFF_MILLIS_KEY = "retry.backoff.millis";
    static final String RETRY_BACKOFF_MILLIS_DOC = "The amount of time to wait between Lambda invocation retry attempts";
    static final int RETRY_BACKOFF_MILLIS_DEFAULT = 500;

    static final String RETRIABLE_ERROR_CODES_KEY = "retriable.error.codes";
    static final String RETRIABLE_ERROR_CODES_DOC = "A comma-separated list with the error codes which cause a Lambda invocation retry";
    static final String RETRIABLE_ERROR_CODES_DEFAULT = "500,503,504";

    private final String connectorName;
    private final Collection<Integer> retriableErrorCodes;
    private final InvocationClientConfig invocationClientConfig;
    private final PayloadFormatterConfig payloadFormatterConfig;

    LambdaSinkConnectorConfig(final Map<String, String> parsedConfig) {
        super(configDef(), parsedConfig);
        this.connectorName = parsedConfig.getOrDefault(CONNECTOR_NAME_KEY,
            "LambdaSinkConnector-Unnamed-" + ThreadLocalRandom.current()
            .ints(4)
            .mapToObj(String::valueOf)
            .collect(Collectors.joining()));
        this.retriableErrorCodes = loadRetriableErrorCodes();
        this.invocationClientConfig = new InvocationClientConfig(parsedConfig);
        this.payloadFormatterConfig = new PayloadFormatterConfig(parsedConfig);
    }

    public String getConnectorName() {
        return this.connectorName;
    }

    public boolean isBatchingEnabled() {
        return this.getBoolean(BATCH_RECORDS_ENABLED_KEY);
    }

    public long getRetryBackoffTimeMillis() {
        return  this.getInt(RETRY_BACKOFF_MILLIS_KEY);
    }

    public int getRetries() {
        return this.getInt(RETRIES_MAX_KEY);
    }

    public Collection<Integer> getRetriableErrorCodes() {
        return this.retriableErrorCodes;
    }

    public InvocationClientConfig getInvocationClientConfig() {
        return invocationClientConfig;
    }

    public InvocationClient getInvocationClient() {
        return invocationClientConfig.getInvocationClient();
    }

    public PayloadFormatterConfig getPayloadFormatterConfig() {
        return payloadFormatterConfig;
    }

    public PayloadFormatter getPayloadFormatter() {
        return payloadFormatterConfig.getPayloadFormatter();
    }

    Collection<Integer> loadRetriableErrorCodes() {
       final List<String> retriableErrorCodesString = this.getList(RETRIABLE_ERROR_CODES_KEY);
       try {
           return retriableErrorCodesString
                 .stream()
                 .map(Integer::parseInt)
                 .collect(Collectors.toList());
       } catch (final NumberFormatException e) {
           final String errorMessage = MessageFormat
                 .format("The list {1} was not able to parse to a list of integers",
                         retriableErrorCodesString.stream().collect(Collectors.joining(",")));
           throw new ConfigException(errorMessage, e);
       }
    }

    public static ConfigDef configDef() {
        ConfigDef configDef = new ConfigDef()
            .define(BATCH_RECORDS_ENABLED_KEY,
                Type.BOOLEAN,
                BATCH_RECORDS_ENABLED_DEFAULT,
                Importance.MEDIUM,
                BATCH_RECORDS_ENABLED_DOC)

            .define(RETRIES_MAX_KEY,
                Type.INT,
                RETRIES_DEFAULT,
                Importance.LOW,
                RETRIES_MAX_DOC)

            .define(RETRY_BACKOFF_MILLIS_KEY,
                Type.INT,
                RETRY_BACKOFF_MILLIS_DEFAULT,
                Importance.LOW,
                RETRY_BACKOFF_MILLIS_DOC)

            .define(RETRIABLE_ERROR_CODES_KEY,
                Type.LIST,
                RETRIABLE_ERROR_CODES_DEFAULT,
                Importance.LOW,
                RETRIABLE_ERROR_CODES_DOC);

        InvocationClientConfig.configDef(configDef);
        PayloadFormatterConfig.configDef(configDef);
        return configDef;
    }
}
