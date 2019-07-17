package com.nordstrom.kafka.connect.lambda;

import com.nordstrom.kafka.connect.utils.Facility;

import java.util.Optional;

import static com.nordstrom.kafka.connect.lambda.InvocationFailure.DROP;

public class Configuration {

    private static final int MAX_HTTP_PORT_NUMBER = 65536;
    private final Optional<String> credentialsProfile;
    private final Optional<String> httpProxyHost;
    private final Optional<Integer> httpProxyPort;
    private final Optional<String> awsRegion;
    private final Optional<InvocationFailure> failureMode;
    private final Optional<String> roleArn;
    private final Optional<String> sessionName;
    private final Optional<String> externalId;


    public Configuration(final String credentialsProfile, final String httpProxyHost,
                         final Integer httpProxyPort, final String awsRegion,
                         final InvocationFailure failureMode,
                         final String roleArn, final String sessionName,
                         final String externalId) {
        /*
         * String awsCredentialsProfile =
         * System.getenv(CREDENTIALS_PROFILE_CONFIG_ENV); String awsProxyHost =
         * System.getenv(_CONFIG_ENV); String awsProxyPort =
         * System.getenv(HTTP_PROXY_PORT_CONFIG_ENV);
         *
         */
        this.credentialsProfile =
                Facility.isNotNullNorEmpty(credentialsProfile) ? Optional.of(credentialsProfile)
                        : Optional.empty();
        this.httpProxyHost =
                Facility.isNotNullNorEmpty(httpProxyHost) ? Optional.of(httpProxyHost) : Optional.empty();
        this.httpProxyPort = Facility.isNotNullAndInRange(httpProxyPort, 0, MAX_HTTP_PORT_NUMBER)
                ? Optional.of(httpProxyPort) : Optional.empty();
        this.awsRegion =
                Facility.isNotNullNorEmpty(awsRegion) ? Optional.of(awsRegion) : Optional.empty();
        this.failureMode = Facility.isNotNull(failureMode) ? Optional.of(failureMode): Optional.of(DROP);
        this.roleArn =
                Facility.isNotNullNorEmpty(roleArn) ? Optional.of(roleArn) : Optional.empty();
        this.sessionName =
                Facility.isNotNullNorEmpty(sessionName) ? Optional.of(sessionName) : Optional.empty();
        this.externalId =
                Facility.isNotNullNorEmpty(externalId) ? Optional.of(externalId) : Optional.empty();

    }

    public Configuration(final Optional<String> awsCredentialsProfile,
                         final Optional<String> httpProxyHost,
                         final Optional<Integer> httpProxyPort,
                         final Optional<String> awsRegion,
                         final Optional<InvocationFailure> failureMode,
                         final Optional<String> roleArn,
                         final Optional<String> sessionName,
                         final Optional<String> externalId) {

        this.credentialsProfile = awsCredentialsProfile;
        this.httpProxyHost = httpProxyHost;
        this.httpProxyPort = httpProxyPort;
        this.awsRegion = awsRegion;
        this.failureMode = failureMode;
        this.roleArn = roleArn;
        this.sessionName = sessionName;
        this.externalId = externalId;
    }

    public static Configuration empty() {
        return new Configuration(
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    public Optional<String> getCredentialsProfile() {
        return this.credentialsProfile;
    }

    public Optional<String> getHttpProxyHost() {
        return this.httpProxyHost;
    }

    public Optional<Integer> getHttpProxyPort() {
        return this.httpProxyPort;
    }

    public Optional<String> getAwsRegion() { return this.awsRegion; }

    public Optional<InvocationFailure> getFailureMode() { return this.failureMode; }

    public Optional<String> getRoleArn() { return this.roleArn; }

    public Optional<String> getSessionName() { return this.sessionName; }

    public Optional<String> getExternalId() { return this.externalId; }
}
