package com.nordstrom.kafka.connect.lambda;

import com.nordstrom.kafka.connect.About;
import com.nordstrom.kafka.connect.formatters.PayloadFormatter;
import com.nordstrom.kafka.connect.formatters.PayloadFormattingException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LambdaSinkTask extends SinkTask {
  private static Logger LOGGER = LoggerFactory.getLogger(LambdaSinkTask.class);

  private final Queue<SinkRecord> batchRecords = new ConcurrentLinkedQueue<>();
  private final AtomicInteger retryCount = new AtomicInteger(0);
  private final static int maxBatchSizeBytes = (6 * 1024 * 1024) - 1;

  private String connectorName;
  private InvocationClient invocationClient;
  private PayloadFormatter payloadFormatter;
  private Boolean isBatchingRecords;
  private int maxRetryCount;
  private long retryBackoffMs;
  private Collection<Integer> retriableErrorCodes;

  @Override
  public String version() {
    return About.CURRENT_VERSION;
  }

  @Override
  public void start(final Map<String, String> settings) {
    LambdaSinkConnectorConfig config = new LambdaSinkConnectorConfig(settings);
    this.connectorName = config.getConnectorName();

    LOGGER.info("Starting lambda connector {} task.", connectorName);

    this.invocationClient = config.getInvocationClient();
    this.payloadFormatter = config.getPayloadFormatter();
    this.isBatchingRecords = config.isBatchingEnabled();
    this.maxRetryCount = config.getRetries();
    this.retryBackoffMs = config.getRetryBackoffTimeMillis();
    this.retriableErrorCodes = config.getRetriableErrorCodes();

    LOGGER.info("Context for connector {} task, Assignments[{}], ",
            connectorName,
            this.context
                    .assignment()
                    .stream()
                    .map(tp -> tp.topic() + ":" + tp.partition())
                    .collect(Collectors.joining(","))
    );
  }

  @Override
  public void put(final Collection<SinkRecord> records) {
    if (records == null || records.isEmpty()) {
      LOGGER.debug("No records to process.  connector=\"{}\"", this.connectorName);
      return;
    }

    if (this.isBatchingRecords) {
      this.batchRecords.addAll(records);
      final int batchLength = this.getPayload(this.batchRecords).getBytes().length;
      if (batchLength >= maxBatchSizeBytes) {
        LOGGER.warn("Batch size reached {} bytes within {} records. connector=\"{}\"",
                batchLength,
                this.batchRecords.size(),
                this.connectorName);
        this.rinse();
        this.context.requestCommit();
      }
    }
    else {
      for (final SinkRecord record : records) {
        this.invoke(this.getPayload(record));
      }
      this.context.requestCommit();
    }
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    this.rinse();
  }

  private void rinse() {
    final List<SinkRecord> records = new ArrayList<>(this.batchRecords);

    if (! records.isEmpty()) {

      this.splitBatch(records, maxBatchSizeBytes)
              .forEach(recordsToFlush -> {

                final InvocationResponse response = this.invoke(this.getPayload(recordsToFlush));

                final String responsesMsg = recordsToFlush.stream().map(r -> MessageFormat
                        .format(
                                "key=\"{0}\" topic=\"{1}\" partition=\"{2}\" offset=\"{3}\"",
                                r.key(),
                                r.topic(),
                                r.kafkaPartition(),
                                r.kafkaOffset()))
                        .collect(Collectors.joining(" | "));
                if (responsesMsg != null && !responsesMsg.isEmpty()) {
                  final String message = MessageFormat.format(
                          "Response Summary Batch - connector=\"{0}\" recordcount=\"{1}\" responseCode=\"{2}\" response=\"{3}\" start=\"{4}\" durationtimemillis=\"{5}\" | {6}",
                          this.connectorName,
                          recordsToFlush.size(),
                          response.getStatusCode(),
                          String.join(" : ", response.getResponseString(), response.getErrorString()),
                          response.getStart(),
                          response.getEnd().toEpochMilli() - response.getStart().toEpochMilli(),
                          responsesMsg);
                  LOGGER.info(message);

                  this.batchRecords.removeAll(recordsToFlush);
                }
              });

      if (!this.batchRecords.isEmpty()) {
        LOGGER.error("Race Condition Found between sinkConnector.put() and sinkConnector.flush() connector=\"{}\"", this.connectorName);
      }

    }
    else {
      LOGGER.info("No records sent in the flush cycle. connector=\"{}\"", connectorName);
    }
    this.context.requestCommit();
  }

  private Collection<Collection<SinkRecord>> splitBatch(final List<SinkRecord> records,
                                                        final int maxBatchSizeBytes) {
    final List<Collection<SinkRecord>> result = new ArrayList<>();
    final int batchLength = this.getPayload(records).getBytes().length;

    if (records.size() <= 1 || batchLength < maxBatchSizeBytes) {
      result.add(records);
      return result;
    }

    final int middle = records.size() / 2;
    final int size = records.size();

    final List<SinkRecord> lower = records.subList(0, middle);
    final List<SinkRecord> upper = records.subList(middle, size);

    LOGGER.info("Splitting batch of {} records and {} bytes into 1) {} records  2) {} records",
            records.size(),
            batchLength,
            lower.size(),
            upper.size()
    );

    result.addAll(this.splitBatch(lower, maxBatchSizeBytes));
    result.addAll(this.splitBatch(upper, maxBatchSizeBytes));

    return result;
  }

  private String getPayload(final SinkRecord record) {
    try {
      final String formatted = payloadFormatter.format(record);
      LOGGER.debug("formatted-payload:|{}|", formatted);
      return formatted;
    } catch (final PayloadFormattingException e) {
      throw new DataException("Record could not be formatted.", e);
    }
  }

  private String getPayload(final Collection<SinkRecord> records) {
    try {
      return this.payloadFormatter.format(records);
    } catch (final PayloadFormattingException e) {
      throw new DataException("Records could not be formatted.", e);
    }
  }

  private InvocationResponse invoke(final String payload) {
    final InvocationResponse response = invocationClient.invoke(payload.getBytes());
    final String traceMessage = MessageFormat
            .format("AWS LAMBDA response {0} {1} {2}.",
                    response.getStatusCode(),
                    response.getErrorString(),
                    response.getResponseString()
            );
    LOGGER.trace(traceMessage);

    this.handleResponse(response, this.retryCount,
        this.retriableErrorCodes, this.maxRetryCount, this.retryBackoffMs);
    return response;
  }

  private void handleResponse(
          final InvocationResponse response,
          final AtomicInteger retryCount,
          final Collection<Integer> retriableErrorCodes,
          final int maxRetries,
          final long backoffTimeMs) {
    if (response.getStatusCode() < 300 && response.getStatusCode() >= 200) {
      //success
      retryCount.set(0);
    } else {
      //failure
      if (retriableErrorCodes.contains(response.getStatusCode())) {
        // is a retriable error
        if (retryCount.get() > maxRetries) {
          final String message = MessageFormat
                  .format("OutOfRetriesError with last call {0} {1} {2}",
                          response.getStatusCode(),
                          response.getErrorString(),
                          response.getResponseString()
                  );
          LOGGER.error(message);
          throw new OutOfRetriesException(message);
        } //else

        final String message = MessageFormat
                .format("Retriable Error with last call {0} {1} {2} - retying in {3} ms",
                        response.getStatusCode(),
                        response.getErrorString(),
                        response.getResponseString(),
                        backoffTimeMs
                );
        LOGGER.warn(message);
        retryCount.incrementAndGet();
        this.context.timeout(backoffTimeMs);
        throw new RetriableException(message);
      }

      // NOT retrying -> data loss
      final String message = MessageFormat
              .format("Non-retriable Error with last call {0} {1} {2} ",
                      response.getStatusCode(),
                      response.getErrorString(),
                      response.getResponseString()
              );
      LOGGER.error(message);
    }

  }

  @Override
  public void stop() {
    LOGGER.info("Stopping lambda connector {} task.", this.connectorName);
  }

  private class OutOfRetriesException extends RuntimeException {
    OutOfRetriesException(final String message) {
      super(message);
    }
  }
}
