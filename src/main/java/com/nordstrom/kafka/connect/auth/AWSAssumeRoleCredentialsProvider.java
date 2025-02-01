package com.nordstrom.kafka.connect.auth;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import org.apache.kafka.common.Configurable;

import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class AWSAssumeRoleCredentialsProvider implements AwsCredentialsProvider, Configurable {
//  Uncomment slf4j imports and field declaration to enable logging.
//  private static final Logger log = LoggerFactory.getLogger(AWSAssumeRoleCredentialsProvider.class);

  public static final String EXTERNAL_ID_CONFIG = "external.id";
  public static final String ROLE_ARN_CONFIG = "role.arn";
  public static final String SESSION_NAME_CONFIG = "session.name";

  private String externalId;
  private String roleArn;
  private String sessionName;

  public static AWSAssumeRoleCredentialsProvider create() {
    return new AWSAssumeRoleCredentialsProvider();
  }

  @Override
  public void configure(Map<String, ?> map) {
    externalId = getOptionalField(map, EXTERNAL_ID_CONFIG);
    roleArn = getRequiredField(map, ROLE_ARN_CONFIG);
    sessionName = getRequiredField(map, SESSION_NAME_CONFIG);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AwsCredentialsProvider provider = StsAssumeRoleCredentialsProvider.builder()
      .stsClient(StsClient.create())
      .refreshRequest(r -> r
        .externalId(externalId)
        .roleArn(roleArn)
        .roleSessionName(sessionName))
      .build();

    return provider.resolveCredentials();
  }

  String getOptionalField(final Map<String, ?> map, final String fieldName) {
    final Object field = map.get(fieldName);
    if (isNotNull(field)) {
      return field.toString();
    }
    return null;
  }

  String getRequiredField(final Map<String, ?> map, final String fieldName) {
    final Object field = map.get(fieldName);
    verifyNotNull(field, fieldName);
    final String fieldValue = field.toString();
    verifyNotNullOrEmpty(fieldValue, fieldName);

    return fieldValue;
  }

  private boolean isNotNull(final Object field) {
    return null != field;
  }

  private boolean isNotNullOrEmpty(final String field) {
    return null != field && !field.isEmpty();
  }

  private void verifyNotNull(final Object field, final String fieldName) {
    if (!isNotNull(field)) {
      throw new IllegalArgumentException(String.format("The field '%1s' should not be null", fieldName));
    }
  }

  private void verifyNotNullOrEmpty(final String field, final String fieldName) {
    if (!isNotNullOrEmpty(field)) {
      throw new IllegalArgumentException(String.format("The field '%1s' should not be null or empty", fieldName));
    }
  }

  public String getExternalId() {
    return this.externalId;
  }

  public String getRoleArn() {
    return this.roleArn;
  }

  public String getSessionName() {
    return this.sessionName;
  }
}
