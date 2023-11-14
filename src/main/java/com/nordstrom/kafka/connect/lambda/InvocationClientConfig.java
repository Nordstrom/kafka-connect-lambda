package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.nordstrom.kafka.connect.auth.AWSAssumeRoleCredentialsProvider;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InvocationClientConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(InvocationClient.class);
    static final String CONFIG_GROUP_NAME = "Lambda";

    static final String AWS_REGION_KEY = "aws.region";
    static final String AWS_REGION_DOC = "AWS region of the Lambda function";
    static final String FUNCTION_ARN_KEY = "aws.lambda.function.arn";
    static final String FUNCTION_ARN_DOC = "Full ARN of the function to be called";
    static final String INVOCATION_MODE_KEY = "aws.lambda.invocation.mode";
    static final String INVOCATION_MODE_DOC = "Determines whether to invoke the lambda asynchronously (Event) or synchronously (RequestResponse)";
    static final String INVOCATION_TIMEOUT_KEY = "aws.lambda.invocation.timeout.ms";
    static final String INVOCATION_TIMEOUT_DOC = "Time to wait for a response after invoking a lambda. If the response times out, the connector will continue.";
    static final String FAILURE_MODE_KEY = "aws.lambda.invocation.failure.mode";
    static final String FAILURE_MODE_DOC = "Determines whether the connector should stop or drop and continue on failure (specifically, payload limit exceeded)";

    // Client configuration properties
    static final String HTTP_PROXY_HOST_KEY = "http.proxy.host";
    static final String HTTP_PROXY_HOST_DOC = "HTTP proxy host to use when invoking the Lambda API";
    static final String HTTP_PROXY_PORT_KEY = "http.proxy.port";
    static final String HTTP_PROXY_PORT_DOC = "HTTP proxy port to use when invoking the Lambda API";

    // Authentication properties
    static final String CREDENTIALS_PROVIDER_CONFIG_PREFIX = "aws.credentials.provider.";
    static final String CREDENTIALS_PROVIDER_CLASS_KEY = "aws.credentials.provider.class";
    static final String CREDENTIALS_PROVIDER_CLASS_DOC = "Implementation class which provides AWS authentication credentials";
    static final String IAM_ROLE_ARN_KEY = CREDENTIALS_PROVIDER_CONFIG_PREFIX + AWSAssumeRoleCredentialsProvider.ROLE_ARN_CONFIG;
    static final String IAM_ROLE_ARN_DOC = "Full ARN of an IAM role to assume";
    static final String IAM_SESSION_NAME_KEY = CREDENTIALS_PROVIDER_CONFIG_PREFIX + AWSAssumeRoleCredentialsProvider.SESSION_NAME_CONFIG;
    static final String IAM_SESSION_NAME_DOC = "IAM session name to use when assuming an IAM role";
    static final String IAM_EXTERNAL_ID_KEY = CREDENTIALS_PROVIDER_CONFIG_PREFIX + AWSAssumeRoleCredentialsProvider.EXTERNAL_ID_CONFIG;
    static final String IAM_EXTERNAL_ID_DOC = "External ID to use when assuming an IAM role";

    final InvocationClient.Builder clientBuilder;
    boolean isLocalstackEnabled;

    InvocationClientConfig(final Map<String, String> parsedConfig) {
        this(new InvocationClient.Builder(), parsedConfig);
    }

    InvocationClientConfig(final Map<String, String> parsedConfig, boolean valor) {
        this(new InvocationClient.Builder(valor), parsedConfig);
    }

    InvocationClientConfig(final InvocationClient.Builder builder, final Map<String, String> parsedConfig) {
        super(configDef(), parsedConfig);

        builder
            .setFunctionArn(getString(FUNCTION_ARN_KEY))
            .setInvocationMode(InvocationMode.valueOf(getString(INVOCATION_MODE_KEY)))
            .setInvocationTimeout(Duration.ofMillis(getLong(INVOCATION_TIMEOUT_KEY)))
            .setFailureMode(InvocationFailure.valueOf(getString(FAILURE_MODE_KEY)))
            .withClientConfiguration(loadAwsClientConfiguration())
            .withCredentialsProvider(loadAwsCredentialsProvider());

        String awsRegion = getString(AWS_REGION_KEY);

        isLocalstackEnabled = Boolean.parseBoolean(parsedConfig.get(LambdaSinkConnectorConfig.LOCALSTACK_ENABLED_KEY));
        if(!isLocalstackEnabled && awsRegion != null) {
            builder.setRegion(awsRegion);
        }

        this.clientBuilder = builder;
    }

    public InvocationClient getInvocationClient() {
        return this.clientBuilder.build();
    }

    ClientConfiguration loadAwsClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        String httpProxyHost = this.getString(HTTP_PROXY_HOST_KEY);
        if (httpProxyHost != null && !httpProxyHost.isEmpty()) {
            clientConfiguration.setProxyHost(httpProxyHost);

            Integer httpProxyPort = this.getInt(HTTP_PROXY_PORT_KEY);
            if (httpProxyPort > 0)
                clientConfiguration.setProxyPort(httpProxyPort);
        }

        return clientConfiguration;
    }

    @SuppressWarnings("unchecked")
    AWSCredentialsProvider loadAwsCredentialsProvider() {
        try {
            AWSCredentialsProvider credentialsProvider = ((Class<? extends AWSCredentialsProvider>)
                getClass(CREDENTIALS_PROVIDER_CLASS_KEY)).getDeclaredConstructor().newInstance();

            if (credentialsProvider instanceof Configurable) {
                Map<String, Object> configs = originalsWithPrefix(
                    CREDENTIALS_PROVIDER_CONFIG_PREFIX);

                ((Configurable)credentialsProvider).configure(configs);
            }

            return credentialsProvider;

        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            throw new ConnectException("Unable to create " + CREDENTIALS_PROVIDER_CLASS_KEY, e);
        }
    }

    public static ConfigDef configDef() {
        return configDef(new ConfigDef());
    }

    public static ConfigDef configDef(ConfigDef base) {
        int orderInGroup = 0;

        return new ConfigDef(base)
            .define(AWS_REGION_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                AWS_REGION_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "AWS region")

            .define(FUNCTION_ARN_KEY,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                FUNCTION_ARN_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Lambda function ARN")

            .define(INVOCATION_MODE_KEY,
                ConfigDef.Type.STRING,
                InvocationClient.DEFAULT_INVOCATION_MODE.name(),
                new InvocationModeValidator(),
                ConfigDef.Importance.MEDIUM,
                INVOCATION_MODE_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Invocation mode",
                new InvocationModeRecommender())

            .define(INVOCATION_TIMEOUT_KEY,
                ConfigDef.Type.LONG,
                (Long)InvocationClient.DEFAULT_INVOCATION_TIMEOUT_MS,
                ConfigDef.Importance.LOW,
                INVOCATION_TIMEOUT_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Invocation timeout")

            .define(FAILURE_MODE_KEY,
                ConfigDef.Type.STRING,
                InvocationClient.DEFAULT_FAILURE_MODE.name(),
                new InvocationFailureValidator(),
                ConfigDef.Importance.LOW,
                FAILURE_MODE_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "Invocation failure mode",
                new InvocationFailureRecommender())

            .define(HTTP_PROXY_HOST_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                HTTP_PROXY_HOST_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "HTTP proxy host")

            .define(HTTP_PROXY_PORT_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                HTTP_PROXY_PORT_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "HTTP proxy port")

            .define(CREDENTIALS_PROVIDER_CLASS_KEY,
                ConfigDef.Type.CLASS,
                DefaultAWSCredentialsProviderChain.class,
                new AwsCredentialsProviderValidator(),
                ConfigDef.Importance.LOW,
                CREDENTIALS_PROVIDER_CLASS_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "AWS credentials provider class")

            .define(IAM_ROLE_ARN_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                IAM_ROLE_ARN_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "IAM role ARN")

            .define(IAM_SESSION_NAME_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                IAM_SESSION_NAME_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "IAM session name")

            .define(IAM_EXTERNAL_ID_KEY,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                IAM_EXTERNAL_ID_DOC,
                CONFIG_GROUP_NAME,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                "IAM external ID");
    }

    static class InvocationModeRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.asList(InvocationMode.values());
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    static class InvocationModeValidator implements ConfigDef.Validator {
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

    static class InvocationFailureRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.asList(InvocationFailure.values());
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    static class InvocationFailureValidator implements ConfigDef.Validator {
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

    static class AwsCredentialsProviderValidator implements ConfigDef.Validator {
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
}
