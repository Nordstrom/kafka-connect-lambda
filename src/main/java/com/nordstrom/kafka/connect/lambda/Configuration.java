package com.nordstrom.kafka.connect.lambda;

import java.util.Optional;

public class Configuration {

    private static final int MAX_HTTP_PORT_NUMBER = 65536;
    private final Optional<String> credentialsProfile;
    private final Optional<String> httpProxyHost;
    private final Optional<Integer> httpProxyPort;

    public Configuration(final String credentialsProfile, final String httpProxyHost,
                         final Integer httpProxyPort) {
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
    }

    public Configuration(final Optional<String> awsCredentialsProfile,
                         final Optional<String> httpProxyHost,
                         final Optional<Integer> httpProxyPort) {

        this.credentialsProfile = awsCredentialsProfile;
        this.httpProxyHost = httpProxyHost;
        this.httpProxyPort = httpProxyPort;
    }

    public static Configuration empty() {
        return new Configuration(Optional.empty(), Optional.empty(), Optional.empty());
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

}
