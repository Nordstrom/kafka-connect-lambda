package com.nordstrom.kafka.connect.lambda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkRecordSerializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkRecordSerializable.class);

    private final ObjectWriter jsonWriter = new ObjectMapper()
            .writerFor(SinkRecordSerializable.class);
    private String value;
    private long offset;
    private long timestamp;
    private String timestampTypeName;
    private int partition;
    private String key;
    private String keySchemaName;
    private String valueSchemaName;
    private String topic;

    public SinkRecordSerializable(final SinkRecord record) {
        super();
        this.key = record.key() == null ? "" : record.key().toString();
        this.keySchemaName = record.keySchema().name();

        this.value = record.value() == null ? "" : record.value().toString();
        this.valueSchemaName = record.valueSchema().name();

        this.topic = record.topic();
        this.partition = record.kafkaPartition();
        this.offset = record.kafkaOffset();

        this.timestamp = record.timestamp();
        this.timestampTypeName = record.timestampType().name;

    }

    public String getValue() {
        return this.value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestampTypeName() {
        return this.timestampTypeName;
    }

    public void setTimestampTypeName(final String timestampTypeName) {
        this.timestampTypeName = timestampTypeName;
    }

    public int getPartition() {
        return this.partition;
    }

    public void setPartition(final int partition) {
        this.partition = partition;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public String getKeySchemaName() {
        return this.keySchemaName;
    }

    public void setKeySchemaName(final String keySchemaName) {
        this.keySchemaName = keySchemaName;
    }

    public String getValueSchemaName() {
        return this.valueSchemaName;
    }

    public void setValueSchemaName(final String valueSchemaName) {
        this.valueSchemaName = valueSchemaName;
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String toJsonString() {
        try {
            return this.jsonWriter.writeValueAsString(this);
        } catch (final JsonProcessingException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new KafkaSerializationException(e);
        }

    }
}
