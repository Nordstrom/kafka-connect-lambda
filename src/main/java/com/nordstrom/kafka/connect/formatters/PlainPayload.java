package com.nordstrom.kafka.connect.formatters;

import org.apache.kafka.connect.sink.SinkRecord;

public class PlainPayload {
    private String key;
    private String keySchemaName;
    private String value;
    private String valueSchemaName;
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private String timestampTypeName;

    protected PlainPayload() {
    }

    public PlainPayload(final SinkRecord record) {
        this.key = record.key() == null ? "" : record.key().toString();
        if (record.keySchema() != null)
            this.keySchemaName = record.keySchema().name();

        this.value = record.value() == null ? "" : record.value().toString();
        if (record.valueSchema() != null)
            this.valueSchemaName = record.valueSchema().name();

        this.topic = record.topic();
        this.partition = record.kafkaPartition();
        this.offset = record.kafkaOffset();

        if (record.timestamp() != null)
            this.timestamp = record.timestamp();
        if (record.timestampType() != null)
            this.timestampTypeName = record.timestampType().name;
    }

    public String getValue() { return this.value; }
    public void setValue(final String value) { this.value = value; }

    public long getOffset() { return this.offset; }
    public void setOffset(final long offset) { this.offset = offset; }

    public Long getTimestamp() { return this.timestamp; }
    public void setTimestamp(final long timestamp) { this.timestamp = timestamp; }

    public String getTimestampTypeName() { return this.timestampTypeName; }
    public void setTimestampTypeName(final String timestampTypeName) { this.timestampTypeName = timestampTypeName; }

    public int getPartition() { return this.partition; }
    public void setPartition(final int partition) { this.partition = partition; }

    public String getKey() { return this.key; }
    public void setKey(final String key) { this.key = key; }

    public String getKeySchemaName() { return this.keySchemaName; }
    public void setKeySchemaName(final String keySchemaName) { this.keySchemaName = keySchemaName; }

    public String getValueSchemaName() { return this.valueSchemaName; }
    public void setValueSchemaName(final String valueSchemaName) { this.valueSchemaName = valueSchemaName; }

    public String getTopic() { return this.topic; }
    public void setTopic(final String topic) { this.topic = topic; }
}
