package com.nordstrom.kafka.connect.formatters;

import org.apache.kafka.connect.sink.SinkRecord;

// TODO: Factor-out PlainPayload with this Generic type.
public class Payload<KType, VType> {
  private KType key;
  private String keySchemaName;
  private String keySchemaVersion;
  private VType value;
  private String valueSchemaName;
  private String valueSchemaVersion;
  private String topic;
  private int partition;
  private long offset;
  private long timestamp;
  private String timestampTypeName;

  protected Payload() {
  }

  public Payload(final SinkRecord record) {
    if (record.keySchema() != null) {
      this.keySchemaName = record.keySchema().name();
      if (record.keySchema().version() != null ) {
        this.keySchemaVersion = record.keySchema().version().toString();
      }
    }

    if (record.valueSchema() != null) {
      this.valueSchemaName = record.valueSchema().name();
      if (record.valueSchema().version() != null ) {
        this.valueSchemaVersion = record.valueSchema().version().toString();
      }
    }

    this.topic = record.topic();
    this.partition = record.kafkaPartition();
    this.offset = record.kafkaOffset();

    if (record.timestamp() != null) {
      this.timestamp = record.timestamp();
    }
    if (record.timestampType() != null) {
      this.timestampTypeName = record.timestampType().name;
    }

  }

  //
  // getters/setters
  //
  public KType getKey() {
    return key;
  }

  public void setKey(KType key) {
    this.key = key;
  }

  public String getKeySchemaName() {
    return keySchemaName;
  }

  public void setKeySchemaName(String keySchemaName) {
    this.keySchemaName = keySchemaName;
  }

  public String getKeySchemaVersion() {
    return keySchemaVersion;
  }

  public void setKeySchemaVersion(String keySchemaVersion) {
    this.keySchemaVersion = keySchemaVersion;
  }

  public VType getValue() {
    return value;
  }

  public void setValue(VType value) {
    this.value = value;
  }

  public String getValueSchemaName() {
    return valueSchemaName;
  }

  public void setValueSchemaName(String valueSchemaName) {
    this.valueSchemaName = valueSchemaName;
  }

  public String getValueSchemaVersion() {
    return valueSchemaVersion;
  }

  public void setValueSchemaVersion(String valueSchemaVersion) {
    this.valueSchemaVersion = valueSchemaVersion;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getTimestampTypeName() {
    return timestampTypeName;
  }

  public void setTimestampTypeName(String timestampTypeName) {
    this.timestampTypeName = timestampTypeName;
  }

}
