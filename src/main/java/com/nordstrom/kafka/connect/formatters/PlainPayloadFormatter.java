package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainPayloadFormatter implements PayloadFormatter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlainPayloadFormatter.class);
    private final ObjectWriter recordWriter = new ObjectMapper().writerFor(PlainPayload.class);
    private final ObjectWriter recordsWriter = new ObjectMapper().writerFor(PlainPayload[].class);

    public String format(final SinkRecord record) {
        PlainPayload payload = new PlainPayload(record);

        try {
            return this.recordWriter.writeValueAsString(payload);
        } catch (final JsonProcessingException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new PayloadFormattingException(e);
        }
    }

    public String format(final Collection<SinkRecord> records) {
        final PlainPayload[] payloads = records
            .stream()
            .map(record -> new PlainPayload(record))
            .toArray(PlainPayload[]::new);

        try {
            return this.recordsWriter.writeValueAsString(payloads);
        } catch (final JsonProcessingException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new PayloadFormattingException(e);
        }
    }
}
