package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.ClientConfiguration;

import com.nordstrom.kafka.connect.formatters.PayloadFormatter;
import com.nordstrom.kafka.connect.formatters.PlainPayloadFormatter;
import com.nordstrom.kafka.connect.utils.Guard;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ThreadLocalRandom;
import java.lang.reflect.InvocationTargetException;

public class LambdaSinkConnectorConfig extends AbstractConfig {
    private static final String AWS_REGION_DEFAULT = "us-west-2";
    private static final long AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT = 5 * 60 * 1000L;
    private static final String HTTP_PROXY_HOST_DEFAULT = "";
    private static final int HTTP_PROXY_PORT_DEFAULT = -1;
    private static final boolean AWS_LAMBDA_BATCH_ENABLED_DEFAULT = true;
    private static final String AWS_LAMBDA_INVOCATION_MODE_DEFAULT = InvocationMode.SYNC.name();
    private static final String AWS_LAMBDA_INVOCATION_FAILURE_MODE_DEFAULT = InvocationFailure.STOP.name();
    private static final String RETRIABLE_ERROR_CODES_DEFAULT = "500,503,504";
    private static final int RETRY_BACKOFF_MILLIS_DEFAULT = 500;
    private static final int RETRIES_DEFAULT = 5;
    private static final String AWS_IAM_ROLE_ARN_DEFAULT = "";
    private static final String AWS_IAM_SESSION_NAME_DEFAULT = "";
    private static final String AWS_IAM_EXTERNAL_ID_DEFAULT = "";
    private static final boolean PAYLOAD_FORMATTER_SCHEMAS_ENABLE_DEFAULT = true;

    private final String connectorName;
    private final ClientConfiguration awsClientConfiguration;
    private final AWSCredentialsProvider awsCredentialsProvider;
    private final PayloadFormatter payloadFormatter;
    private final Collection<Integer> retriableErrorCodes;

    LambdaSinkConnectorConfig(final Map<String, String> parsedConfig) {
        this(configDef(), parsedConfig);
    }

    LambdaSinkConnectorConfig(final ConfigDef configDef, final Map<String, String> parsedConfig) {
        super(configDef, parsedConfig);

        this.connectorName = parsedConfig.getOrDefault(ConfigurationKeys.NAME_CONFIG.getValue(),
            "LambdaSinkConnector-Unnamed-" + ThreadLocalRandom.current()
            .ints(4)
            .mapToObj(String::valueOf)
            .collect(Collectors.joining()));

        this.awsClientConfiguration = loadAwsClientConfiguration();
        this.awsCredentialsProvider = loadAwsCredentialsProvider();
        this.payloadFormatter = loadPayloadFormatter();
        this.retriableErrorCodes = loadRetriableErrorCodes();
    }

    public String getConnectorName() {
        return this.connectorName;
    }

    public String getAwsFunctionArn() {
        return this.getString(ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getValue());
    }

    public Duration getInvocationTimeout() {
        return Duration.ofMillis(this.getLong(ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getValue()));
    }

    public InvocationMode getInvocationMode() {
        return InvocationMode.valueOf(this.getString(ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getValue()));
    }

    public InvocationFailure getFailureMode() {
        return InvocationFailure.valueOf(this.getString(ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getValue()));
    }

    public boolean isBatchingEnabled() {
        return this.getBoolean(ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getValue());
    }

    public long getRetryBackoffTimeMillis() {
        return  this.getInt(ConfigurationKeys.RETRY_BACKOFF_MILLIS.getValue());
    }

    public int getRetries() {
        return this.getInt(ConfigurationKeys.RETRIES_MAX.getValue());
    }

    public Collection<Integer> getRetriableErrorCodes() {
        return this.retriableErrorCodes;
    }

    Collection<Integer> loadRetriableErrorCodes() {
       final List<String> retriableErrorCodesString = this.getList(ConfigurationKeys.RETRIABLE_ERROR_CODES.getValue());
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

    public String getAwsRegion() {
        return this.getString(ConfigurationKeys.AWS_REGION.getValue());
    }

    public ClientConfiguration getAwsClientConfiguration() {
        return this.awsClientConfiguration;
    }

    ClientConfiguration loadAwsClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        String httpProxyHost = this.getString(ConfigurationKeys.HTTP_PROXY_HOST.getValue());

        if (!httpProxyHost.isEmpty()) {
            clientConfiguration.setProxyHost(httpProxyHost);

            Integer httpProxyPort = this.getInt(ConfigurationKeys.HTTP_PROXY_PORT.getValue());
            if (httpProxyPort != HTTP_PROXY_PORT_DEFAULT)
                clientConfiguration.setProxyPort(httpProxyPort);
        }

        return clientConfiguration;
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return this.awsCredentialsProvider;
    }

    @SuppressWarnings("unchecked")
    AWSCredentialsProvider loadAwsCredentialsProvider() {
        String configKey = ConfigurationKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getValue();

        try {
            AWSCredentialsProvider awsCredentialsProvider = ((Class<? extends AWSCredentialsProvider>)
                getClass(configKey)).getDeclaredConstructor().newInstance();

            if (awsCredentialsProvider instanceof Configurable) {
                Map<String, Object> configs = originalsWithPrefix(
                    ConfigurationKeys.CREDENTIALS_PROVIDER_CONFIG_PREFIX.getValue());

                ((Configurable)awsCredentialsProvider).configure(configs);
            }

            return awsCredentialsProvider;

        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            throw new ConnectException("Unable to create " + configKey, e);
        }
    }

    public String getIamRoleArn() {
      return this.getString(ConfigurationKeys.AWS_IAM_ROLE_ARN_CONFIG.getValue());
    }

    public String getIamSessionName() {
        return this.getString(ConfigurationKeys.AWS_IAM_SESSION_NAME_CONFIG.getValue());
    }
  
    public String getIamExternalId() {
        return this.getString(ConfigurationKeys.AWS_IAM_EXTERNAL_ID_CONFIG.getValue());
    }

    public PayloadFormatter getPayloadFormatter() {
        return this.payloadFormatter;
    }
    public boolean isPayloadFormatterSchemasEnable() {
        return this.getBoolean(ConfigurationKeys.PAYLOAD_FORMATTER_SCHEMAS_ENABLE_CONFIG.getValue());
    }

    @SuppressWarnings("unchecked")
    PayloadFormatter loadPayloadFormatter() {
        String configKey = ConfigurationKeys.PAYLOAD_FORMATTER_CLASS_CONFIG.getValue();

        try {
            PayloadFormatter payloadFormatter = ((Class<? extends PayloadFormatter>)
                getClass(configKey)).getDeclaredConstructor().newInstance();

            if (payloadFormatter instanceof Configurable) {
                Map<String, Object>configs = originalsWithPrefix(
                    ConfigurationKeys.PAYLOAD_FORMATTER_CONFIG_PREFIX.getValue());
                ((Configurable)payloadFormatter).configure(configs);
            }
            return payloadFormatter;

        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            throw new ConnectException("Unable to create " + configKey, e);
        }
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getValue(), Type.STRING, Importance.HIGH,
                ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getDocumentation())

            .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getValue(), Type.LONG,
                AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT, Importance.HIGH,
                ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getDocumentation())

            .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getValue(), Type.STRING,
                AWS_LAMBDA_INVOCATION_MODE_DEFAULT,
                new InvocationModeValidator(),
                Importance.MEDIUM,
                ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getDocumentation(),
                "LAMBDA",
                0,
                ConfigDef.Width.SHORT,
                "Invocation mode",
                new InvocationModeRecommender())
  
            .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getValue(), Type.STRING,
                AWS_LAMBDA_INVOCATION_FAILURE_MODE_DEFAULT,
                new InvocationFailureValidator(),
                Importance.MEDIUM,
                ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getDocumentation(),
                "LAMBDA",
                0,
                ConfigDef.Width.SHORT,
                "Invocation mode",
                new InvocationFailureRecommender())

            .define(ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getValue(), Type.BOOLEAN,
                AWS_LAMBDA_BATCH_ENABLED_DEFAULT, Importance.MEDIUM,
                ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getDocumentation())

            .define(ConfigurationKeys.AWS_REGION.getValue(), Type.STRING, AWS_REGION_DEFAULT,
                Importance.LOW, ConfigurationKeys.AWS_REGION.getDocumentation())

            .define(ConfigurationKeys.HTTP_PROXY_HOST.getValue(), Type.STRING, HTTP_PROXY_HOST_DEFAULT,
                Importance.LOW, ConfigurationKeys.HTTP_PROXY_HOST.getDocumentation())

            .define(ConfigurationKeys.HTTP_PROXY_PORT.getValue(), Type.INT, HTTP_PROXY_PORT_DEFAULT,
                Importance.LOW, ConfigurationKeys.HTTP_PROXY_PORT.getDocumentation())

            .define(ConfigurationKeys.RETRIES_MAX.getValue(), Type.INT, RETRIES_DEFAULT, Importance.MEDIUM,
                ConfigurationKeys.RETRIES_MAX.getDocumentation())

            .define(ConfigurationKeys.RETRY_BACKOFF_MILLIS.getValue(), Type.INT,
                RETRY_BACKOFF_MILLIS_DEFAULT, Importance.MEDIUM,
                ConfigurationKeys.RETRY_BACKOFF_MILLIS.getDocumentation())

            .define(ConfigurationKeys.RETRIABLE_ERROR_CODES.getValue(), Type.LIST,
                RETRIABLE_ERROR_CODES_DEFAULT, Importance.MEDIUM,
                ConfigurationKeys.RETRIABLE_ERROR_CODES.getDocumentation())

            .define(ConfigurationKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getValue(), Type.CLASS,
                ConfigurationKeys.CREDENTIALS_PROVIDER_CLASS_DEFAULT.getValue(),
                new AwsCredentialsProviderValidator(),
                Importance.LOW,
                ConfigurationKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getDocumentation(),
                "LAMBDA",
                0,
                ConfigDef.Width.LONG,
                "AWS credentials provider class")

            .define(ConfigurationKeys.AWS_IAM_ROLE_ARN_CONFIG.getValue(), Type.STRING, AWS_IAM_ROLE_ARN_DEFAULT,
                Importance.LOW, ConfigurationKeys.AWS_IAM_ROLE_ARN_CONFIG.getDocumentation())

            .define(ConfigurationKeys.AWS_IAM_SESSION_NAME_CONFIG.getValue(), Type.STRING, AWS_IAM_SESSION_NAME_DEFAULT,
                Importance.LOW, ConfigurationKeys.AWS_IAM_SESSION_NAME_CONFIG.getDocumentation())

            .define(ConfigurationKeys.AWS_IAM_EXTERNAL_ID_CONFIG.getValue(), Type.STRING, AWS_IAM_EXTERNAL_ID_DEFAULT,
                Importance.LOW, ConfigurationKeys.AWS_IAM_EXTERNAL_ID_CONFIG.getDocumentation())

            .define(ConfigurationKeys.PAYLOAD_FORMATTER_CLASS_CONFIG.getValue(), Type.CLASS,
                PlainPayloadFormatter.class,
                new PayloadFormatterClassValidator(),
                Importance.LOW,
                ConfigurationKeys.PAYLOAD_FORMATTER_CLASS_CONFIG.getDocumentation(),
                "LAMBDA",
                0,
                ConfigDef.Width.LONG,
                "Invocation payload formatter class")

            .define(ConfigurationKeys.PAYLOAD_FORMATTER_SCHEMAS_ENABLE_CONFIG.getValue(),
                Type.BOOLEAN, PAYLOAD_FORMATTER_SCHEMAS_ENABLE_DEFAULT,
                Importance.LOW,
                ConfigurationKeys.PAYLOAD_FORMATTER_SCHEMAS_ENABLE_CONFIG.getDocumentation()
                )
            ;
    }

    enum ConfigurationKeys {
        NAME_CONFIG("name", "Connector Name"),
        AWS_LAMBDA_FUNCTION_ARN("aws.lambda.function.arn", "Full ARN of the function to be called"),
        AWS_LAMBDA_INVOCATION_TIMEOUT_MS("aws.lambda.invocation.timeout.ms",
                "Time to wait for a lambda invocation, if the response times out, the connector will move forward. Default in ms: "
                        + AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT),
        AWS_LAMBDA_INVOCATION_MODE("aws.lambda.invocation.mode",
                "Determines whether the lambda would be called asynchronously (Event) or Synchronously (Request-Response), possible values are: ["
                        + Stream.of(InvocationMode.values()).map(InvocationMode::toString)
                        .collect(Collectors.joining(",")) + "]"),
        AWS_LAMBDA_INVOCATION_FAILURE_MODE("aws.lambda.invocation.failure.mode",
                "Determines whether the lambda should stop or drop and continue on failure (specifically, payload limit exceeded), possible values are: ["
                        + Stream.of(InvocationFailure.values()).map(InvocationFailure::toString)
                        .collect(Collectors.joining(",")) + "]"),

        AWS_LAMBDA_BATCH_ENABLED("aws.lambda.batch.enabled",
                "Boolean that determines if the messages will be batched together before sending them to aws lambda. By default is " + AWS_LAMBDA_BATCH_ENABLED_DEFAULT),
        AWS_REGION("aws.region",
                "AWS region to instantiate the Lambda client Default: " + AWS_REGION_DEFAULT),
  
        HTTP_PROXY_HOST("http.proxy.host",
                "Http proxy port to be configured for the Lambda client, by default is empty"),
        HTTP_PROXY_PORT("http.proxy.port",
                "Http proxy to be configured for the Lambda client, by default is empty"),
        RETRIES_MAX("retries.max", "Max number of times to retry a call"),
        RETRIABLE_ERROR_CODES("retriable.error.codes"
                , "A comma separated list with the error codes to be retried, by default "
                + RETRIABLE_ERROR_CODES_DEFAULT),
        RETRY_BACKOFF_MILLIS("retry.backoff.millis",
                "The amount of time to wait between retry attempts, by default is "
                        + RETRY_BACKOFF_MILLIS_DEFAULT),

        CREDENTIALS_PROVIDER_CLASS_CONFIG("aws.credentials.provider.class", "Class providing cross-account role assumption"),
        CREDENTIALS_PROVIDER_CLASS_DEFAULT("com.amazonaws.auth.DefaultAWSCredentialsProviderChain", "Default provider chain if aws.lambda.credentials.provider.class is not passed in"),
        CREDENTIALS_PROVIDER_CONFIG_PREFIX("aws.credentials.provider.", "Note trailing '.'"),
        AWS_IAM_ROLE_ARN_CONFIG("aws.credentials.provider.role.arn", "REQUIRED AWS Role ARN providing the access"),
        AWS_IAM_SESSION_NAME_CONFIG("aws.credentials.provider.session.name", "REQUIRED Session name"),
        AWS_IAM_EXTERNAL_ID_CONFIG("aws.credentials.provider.external.id", "OPTIONAL (but recommended) External identifier used by the kafka-connect-lambda when assuming the role"),

        PAYLOAD_FORMATTER_CONFIG_PREFIX("payload.", "Note trailing '.'"),
        PAYLOAD_FORMATTER_CLASS_CONFIG("payload.formatter.class", "Class formatter for the invocation payload"),
        PAYLOAD_FORMATTER_SCHEMAS_ENABLE_CONFIG("payload.formatter.schemas.enable", "Include schemas within each of the serialized keys and values")
        ;

        private final String value;
        private final String documentation;

        ConfigurationKeys(final String configurationKeyValue, final String documentation) {
            Guard.verifyNotNullOrEmpty(configurationKeyValue, "configurationKeyValue");

            // Empty or null documentation is ok.
            this.value = configurationKeyValue;
            this.documentation = documentation;
        }

        String getValue() {
            return this.value;
        }

        String getDocumentation() {
            return this.documentation;
        }

        @Override
        public String toString() {
          return this.value;
        }
    }

    private static class InvocationModeRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.asList(InvocationMode.values());
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    private static class InvocationModeValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object invocationMode) {
            try {
                InvocationMode.valueOf(((String)invocationMode).trim());
            } catch (Exception e) {
                throw new ConfigException(name, invocationMode, "Value must be one of [" +
                    Utils.join(InvocationMode.values(), ", ") + "]");
            }
        }

        @Override
        public String toString() {
            return "[" + Utils.join(InvocationMode.values(), ", ") + "]";
        }
    }

    private static class InvocationFailureRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.asList(InvocationFailure.values());
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    private static class InvocationFailureValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object invocationFailure) {
            try {
                InvocationFailure.valueOf(((String)invocationFailure).trim());
            } catch (Exception e) {
                throw new ConfigException(name, invocationFailure, "Value must be one of [" +
                    Utils.join(InvocationFailure.values(), ", ") + "]");
            }
        }

        @Override
        public String toString() {
            return "[" + Utils.join(InvocationFailure.values(), ", ") + "]";
        }
    }

    private static class AwsCredentialsProviderValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object provider) {
          if (provider instanceof Class && AWSCredentialsProvider.class.isAssignableFrom((Class<?>)provider)) {
              return;
          }

          throw new ConfigException(name, provider, "Class must extend: " + AWSCredentialsProvider.class);
        }

        @Override
        public String toString() {
            return "Any class implementing: " + AWSCredentialsProvider.class;
        }
    }

    private static class PayloadFormatterClassValidator implements ConfigDef.Validator {
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
}
