package com.nordstrom.kafka.connect.lambda;

import java.util.Optional;

public class Configuration {

    private static final int MAX_HTTP_PORT_NUMBER = 65536;
    private final Optional<String> credentialsProfile;
    private final Optional<String> httpProxyHost;
    private final Optional<Integer> httpProxyPort;
    private final Optional<String> awsRegion;

    public Configuration(final String credentialsProfile, final String httpProxyHost,
                         final Integer httpProxyPort, final String awsRegion) {
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
    }

    public Configuration(final Optional<String> awsCredentialsProfile,
                         final Optional<String> httpProxyHost,
                         final Optional<Integer> httpProxyPort,
                         final Optional<String> awsRegion) {

        this.credentialsProfile = awsCredentialsProfile;
        this.httpProxyHost = httpProxyHost;
        this.httpProxyPort = httpProxyPort;
        this.awsRegion = awsRegion;
    }

    public static Configuration empty() {
        return new Configuration(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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

}
