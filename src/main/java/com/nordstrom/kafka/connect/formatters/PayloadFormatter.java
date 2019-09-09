package com.nordstrom.kafka.connect.formatters;

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;

public interface PayloadFormatter {
    String format(final SinkRecord record) throws PayloadFormattingException;
    String format(final Collection<SinkRecord> records) throws PayloadFormattingException;
}
