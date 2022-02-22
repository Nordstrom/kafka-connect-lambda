package com.nordstrom.kafka.connect.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.kafka.common.Configurable;
import com.amazonaws.auth.BasicAWSCredentials;

import java.util.Map;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class AWSUserCredentialsProvider implements AWSCredentialsProvider, Configurable {
//  Uncomment slf4j imports and field declaration to enable logging.
//  private static final Logger log = LoggerFactory.getLogger(AWSUserCredentialsProvider.class);

  public static final String ACCESS_KEY_CONFIG = "access.key";
  public static final String SECRET_KEY_CONFIG = "secret.key";

  private String accessKey;
  private String secretKey;

  @Override
  public void configure(Map<String, ?> map) {
    accessKey = getRequiredField(map, ACCESS_KEY_CONFIG);
    secretKey = getRequiredField(map, SECRET_KEY_CONFIG);
  }

  @Override
  public AWSCredentials getCredentials() {
    BasicAWSCredentials provider = new BasicAWSCredentials(accessKey, secretKey);

    return provider;
  }

  @Override
  public void refresh() {
    //Nothing to do really, since we are assuming a role.
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

  public String getAccessKey() {
    return this.accessKey;
  }

  public String getSecretKey() {
    return this.secretKey;
  }
}
