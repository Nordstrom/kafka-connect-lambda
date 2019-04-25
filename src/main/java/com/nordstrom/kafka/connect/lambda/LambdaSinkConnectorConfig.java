package com.nordstrom.kafka.connect.lambda;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LambdaSinkConnectorConfig extends AbstractConfig {

  private static final long AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT = 5 * 60 * 1000L;
  private static final String AWS_REGION_DEFAULT = "us-west-2";
  private static final String AWS_CREDENTIALS_PROFILE_DEFAULT = "";
  private static final String HTTP_PROXY_HOST_DEFAULT = "";
  private static final int HTTP_PROXY_PORT_DEFAULT = -1;
  private static final boolean AWS_LAMBDA_BATCH_ENABLED_DEFAULT = true;
  private static final String AWS_LAMBDA_INVOCATION_MODE_DEFAULT = InvocationMode.SYNC.name();
  private static final String AWS_LAMBDA_INVOCATION_FAILURE_MODE_DEFAULT = InvocationFailure.STOP.name();
  private static final String RETRIABLE_ERROR_CODES_DEFAULT = "500,503,504";
  private static final int RETRY_BACKOFF_MILLIS_DEFAULT = 500;
  private static final int RETRIES_DEFAULT = 5;

  private static final ConfigDef configDefinition = LambdaSinkConnectorConfig.config();

  private static final int MEGABYTE_SIZE = 1024 * 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(LambdaSinkConnectorConfig.class);

  private final Map<String, String> properties;
  private final String connectorName;
  private final String httpProxyHost;
  private final Integer httpProxyPort;
  private final String awsCredentialsProfile;
  private final String awsFunctionArn;
  private final Duration invocationTimeout;
  private final InvocationMode invocationMode;
  private final boolean isBatchingEnabled;
  private final long retryBackoffTimeMillis;
  private final int retries;
  private final Collection<Integer> retriableErrorCodes;
  private final boolean isWithJsonWrapper = true;
  private final int maxBatchSizeBytes = (6 * MEGABYTE_SIZE) - 1;
  private final String awsRegion;
  private final InvocationFailure failureMode;

  public LambdaSinkConnectorConfig(final Map<String, String> properties) {
    this(configDefinition, properties);
  }

  LambdaSinkConnectorConfig(final ConfigDef configDefinition, final Map<String, String> props) {
    super(configDefinition, props);
    this.properties = props;

    this.connectorName = this.properties.getOrDefault(
            ConfigurationKeys.NAME_CONFIG.getValue(),
            "LambdaSinkConnector-Unnamed-" + ThreadLocalRandom.current()
                    .ints(4)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining()));

    this.httpProxyPort = this.getInt(ConfigurationKeys.HTTP_PROXY_PORT.getValue());

    this.httpProxyHost = this.getString(ConfigurationKeys.HTTP_PROXY_HOST.getValue());
    this.awsCredentialsProfile = this
            .getString(ConfigurationKeys.AWS_CREDENTIALS_PROFILE.getValue());

    this.awsFunctionArn = this.getString(ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getValue());
    this.invocationTimeout = Duration.ofMillis(
            this.getLong(ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getValue())
    );

    this.invocationMode = InvocationMode.valueOf(
            this.getString(ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getValue())
    );

    this.isBatchingEnabled = this.getBoolean(ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getValue());
    this.retries = this.getInt(ConfigurationKeys.RETRIES_MAX.getValue());

    final List<String> retriableErrorCodesString = this
            .getList(ConfigurationKeys.RETRIABLE_ERROR_CODES.getValue());
    try {
      this.retriableErrorCodes = retriableErrorCodesString
              .stream()
              .map(Integer::parseInt)
              .collect(Collectors.toList());
    } catch (final NumberFormatException e) {
      final String errorMessage = MessageFormat
              .format("The list {1} was not able to parse to a list of integers",
                      retriableErrorCodesString.stream().collect(Collectors.joining(",")));
      LOGGER.error(errorMessage);
      throw new ConfigException(errorMessage, e);
    }
    this.retryBackoffTimeMillis = this.getInt(ConfigurationKeys.RETRY_BACKOFF_MILLIS.getValue());

    this.awsRegion = this.getString(ConfigurationKeys.AWS_REGION.getValue());

    this.failureMode = InvocationFailure.valueOf(
            this.getString(ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getValue())
    );

  }

  public Map<String, String> getProperties() {
    return this.properties;
  }

  public String getConnectorName() {
    return this.connectorName;
  }

  public String getAwsFunctionArn() {
    return this.awsFunctionArn;
  }

  public Duration getInvocationTimeout() {
    return this.invocationTimeout;
  }

  public String getAwsCredentialsProfile() {
    return this.awsCredentialsProfile;
  }

  public Integer getHttpProxyPort() {
    return this.httpProxyPort;
  }

  public String getHttpProxyHost() {
    return this.httpProxyHost;
  }

  public boolean isWithJsonWrapper() {
    return this.isWithJsonWrapper;
  }

  public InvocationMode getInvocationMode() {
    return this.invocationMode;
  }

  public InvocationFailure getFailureMode() {
    return this.failureMode;
  }

  public boolean isBatchingEnabled() {
    return this.isBatchingEnabled;
  }

  public long getRetryBackoffTimeMillis() {
    return this.retryBackoffTimeMillis;
  }

  public int getRetries() {
    return this.retries;
  }

  public Collection<Integer> getRetriableErrorCodes() {
    return this.retriableErrorCodes;
  }

  public int getMaxBatchSizeBytes() {
    return this.maxBatchSizeBytes;
  }

  public String getAwsRegion() {
    return this.awsRegion;
  }

  static ConfigDef getConfigDefinition() {
    return configDefinition;
  }

  public static ConfigDef config() {
    return new ConfigDef()
      .define(ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getValue(), Type.STRING, Importance.HIGH,
        ConfigurationKeys.AWS_LAMBDA_FUNCTION_ARN.getDocumentation())

      .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getValue(), Type.LONG,
        AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT, Importance.HIGH,
        ConfigurationKeys.AWS_LAMBDA_INVOCATION_TIMEOUT_MS.getDocumentation())

      .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getValue(), Type.STRING,
        AWS_LAMBDA_INVOCATION_MODE_DEFAULT, Importance.MEDIUM,
        ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getDocumentation())

      .define(ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getValue(), Type.STRING,
        AWS_LAMBDA_INVOCATION_FAILURE_MODE_DEFAULT, Importance.MEDIUM,
        ConfigurationKeys.AWS_LAMBDA_INVOCATION_FAILURE_MODE.getDocumentation())

      .define(ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getValue(), Type.BOOLEAN,
              AWS_LAMBDA_BATCH_ENABLED_DEFAULT, Importance.MEDIUM,
              ConfigurationKeys.AWS_LAMBDA_BATCH_ENABLED.getDocumentation())

      .define(ConfigurationKeys.AWS_REGION.getValue(), Type.STRING, AWS_REGION_DEFAULT,
              Importance.LOW, ConfigurationKeys.AWS_REGION.getDocumentation())

      .define(ConfigurationKeys.AWS_CREDENTIALS_PROFILE.getValue(), Type.STRING,
              AWS_CREDENTIALS_PROFILE_DEFAULT, Importance.LOW,
              ConfigurationKeys.AWS_CREDENTIALS_PROFILE.getDocumentation())

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
              ConfigurationKeys.RETRIABLE_ERROR_CODES.getDocumentation());
  }

  public enum ConfigurationKeys {
    NAME_CONFIG("name", "Connector Name"),
    TASK_ID("task.id", "Connector Task id"),
    AWS_LAMBDA_FUNCTION_ARN("aws.lambda.function.arn", "Full ARN for the function to be called."),
    AWS_LAMBDA_INVOCATION_TIMEOUT_MS("aws.lambda.invocation.timeout.ms",
            "Time to wait for a lambda invocation, if the response times out, the connector will move forward. Default in ms: "
                    + AWS_LAMBDA_INVOCATION_TIMEOUT_MS_DEFAULT),
    AWS_LAMBDA_INVOCATION_MODE("aws.lambda.invocation.mode",
            "Determines whether the lambda would be called asynchronously (Event) or Synchronously (Request-Response), possible values are: "
                    + Stream.of(InvocationMode.values()).map(InvocationMode::toString)
                    .collect(Collectors.joining(","))),
    AWS_LAMBDA_INVOCATION_FAILURE_MODE("aws.lambda.invocation.failure.mode", // TODO Maybe generalize for other types of failures
            "Determines whether the lambda should stop or drop and continue on failure (specifically, payload limit exceeded), possible values are: "
                    + Stream.of(InvocationFailure.values()).map(InvocationFailure::toString)
                    .collect(Collectors.joining(","))),

    AWS_LAMBDA_BATCH_ENABLED("aws.lambda.batch.enabled",
            "Boolean that determines if the messages will be batched together before sending them to aws lambda. By default is "
                    + AWS_LAMBDA_BATCH_ENABLED_DEFAULT
    ),
    AWS_REGION("aws.region",
            "AWS region to instantiate the Lambda client Default: " + AWS_REGION_DEFAULT),
    AWS_CREDENTIALS_PROFILE("aws.credentials.profile",
            " AWS credentials profile to use for the Lambda client, by default is empty and will use the DefaultAWSCredentialsProviderChain "),

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
                    + RETRY_BACKOFF_MILLIS_DEFAULT);


    private final String value;
    private final String documentation;

    ConfigurationKeys(final String configurationKeyValue, final String documentation) {
      Guard.verifyNotNullOrEmpty(configurationKeyValue, "configurationKeyValue");

      // Empty or null documentation is OK.
      this.value = configurationKeyValue;
      this.documentation = documentation;
    }

    public String getDocumentation() {
      return this.documentation;
    }

    public String getValue() {
      return this.value;
    }

    @Override
    public String toString() {
      return this.value;
    }
  }
}
