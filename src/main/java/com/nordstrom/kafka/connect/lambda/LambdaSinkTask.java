package com.nordstrom.kafka.connect.lambda;

import com.nordstrom.kafka.connect.utils.About;
import com.nordstrom.kafka.connect.utils.JsonUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LambdaSinkTask extends SinkTask {
  private static Logger LOGGER = LoggerFactory.getLogger(LambdaSinkTask.class);

  private final Queue<SinkRecord> batchRecords = new ConcurrentLinkedQueue<>();
  private final AtomicInteger retryCount = new AtomicInteger(0);

  AwsLambdaUtil lambdaClient;
  Map<String, String> properties;
  LambdaSinkTaskConfiguration configuration;

  @Override
  public String version() {
    return About.CURRENT_VERSION;
  }

  @Override
  public void start(final Map<String, String> props) {
    this.properties = props;
    this.configuration = new LambdaSinkTaskConfiguration(this.properties);

    LOGGER.info("starting connector {} task {} with properties {}",
            this.configuration.getConnectorName(),
            this.configuration.getTaskId(),
            props);

    Configuration optConfigs =  new Configuration(
                    this.configuration.getAwsCredentialsProfile(),
                    this.configuration.getHttpProxyHost(),
                    this.configuration.getHttpProxyPort(),
                    this.configuration.getAwsRegion(),
                    this.configuration.getFailureMode(),
                    this.configuration.getRoleArn(),
                    this.configuration.getSessionName(),
                    this.configuration.getExternalId());
    this.lambdaClient = new AwsLambdaUtil(optConfigs, configuration.originalsWithPrefix(LambdaSinkConnectorConfig.ConfigurationKeys.CREDENTIALS_PROVIDER_CONFIG_PREFIX.getValue()));

    LOGGER.info("Context for connector {} task {}, Assignments[{}], ",
            this.configuration.getConnectorName(),
            this.configuration.getTaskId(),
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
      LOGGER.info("No records to process.  connector=\"{}\" task=\"{}\"",
              this.configuration.getConnectorName(),
              this.configuration.getTaskId());
      return;
    }

    if (this.configuration.isBatchingEnabled()) {
      this.batchRecords.addAll(records);
      final int batchLength = this.getPayload(this.batchRecords).getBytes().length;
      if (batchLength >= this.configuration.getMaxBatchSizeBytes()) {
        LOGGER.info("Batch size reached {} bytes within {} records. connector=\"{}\" task=\"{}\"",
                batchLength,
                this.batchRecords.size(),
                this.configuration.getConnectorName(),
                this.configuration.getTaskId());
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

  public void setLambdaClient(AwsLambdaUtil lambdaClient) {
    this.lambdaClient = lambdaClient;
  }

  private void rinse() {
    final List<SinkRecord> records = new ArrayList<>(this.batchRecords);

    if (! records.isEmpty()) {

      this.splitBatch(records, this.configuration.getMaxBatchSizeBytes())
              .forEach(recordsToFlush -> {

                final AwsLambdaUtil.InvocationResponse response = this.invoke(this.getPayload(recordsToFlush));

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
                          "Response Summary Batch - arn=\"{0}\", connector=\"{1}\" task=\"{2}\", recordcount=\"{3}\" responseCode=\"{4}\" response=\"{5}\" start=\"{6}\" durationtimemillis=\"{7}\" | {8}",
                          this.configuration.getAwsFunctionArn(),
                          this.configuration.getConnectorName(),
                          this.configuration.getTaskId(),
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
        LOGGER.error(
                "Race Condition Found between sinkConnector.put() and sinkConnector.flush() connector=\"{}\" task=\"{}\"",
                this.configuration.getConnectorName(),
                this.configuration.getTaskId());
      }

    }
    else {
      LOGGER.info("No records sent in the flush cycle. connector=\"{}\" task=\"{}\"",
              this.configuration.getConnectorName(),
              this.configuration.getTaskId());
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
    return this.configuration.isWithJsonWrapper() ?
            new SinkRecordSerializable(record).toJsonString() :
            record.value().toString();
  }

  private String getPayload(final Collection<SinkRecord> records) {
    final List<String> stringRecords = records
            .stream()
            .map(this::getPayload)
            .collect(Collectors.toList());
    return JsonUtil.jsonify(stringRecords);
  }

  private AwsLambdaUtil.InvocationResponse invoke(final String payload) {

    final AwsLambdaUtil.InvocationResponse response;
    switch (this.configuration.getInvocationMode()) {

      case ASYNC:

        response = this.lambdaClient.invokeAsync(
                this.configuration.getAwsFunctionArn(),
                payload.getBytes(),
                this.configuration.getInvocationTimeout()
        );
        break;

      case SYNC:
        response = this.lambdaClient.invokeSync(
                this.configuration.getAwsFunctionArn(),
                payload.getBytes(),
                this.configuration.getInvocationTimeout()
        );
        break;

      default:
        final String message = MessageFormat.format("The {} {} is not defined.",
                LambdaSinkConnectorConfig.ConfigurationKeys.AWS_LAMBDA_INVOCATION_MODE.getValue(),
                this.configuration.getInvocationMode());
        throw new InvalidConfigurationException(message);
    }

    final String traceMessage = MessageFormat
            .format("AWS LAMBDA response {0} {1} {2}.",
                    response.getStatusCode(),
                    response.getErrorString(),
                    response.getResponseString()
            );
    LOGGER.trace(traceMessage);

    this.handleResponse(response, this.retryCount, this.configuration.getRetriableErrorCodes(),
            this.configuration.getRetries(), this.configuration.getRetryBackoffTimeMillis());
    return response;
  }

  private void handleResponse(
          final AwsLambdaUtil.InvocationResponse response,
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
                  .format("OutOfRetriesError with last call {} {} {}",
                          response.getStatusCode(),
                          response.getErrorString(),
                          response.getResponseString()
                  );
          LOGGER.error(message);
          throw new OutOfRetriesException(message);
        } //else

        final String message = MessageFormat
                .format("Retriable Error with last call {} {} {} - retying in {} ms",
                        response.getStatusCode(),
                        response.getErrorString(),
                        response.getResponseString(),
                        backoffTimeMs
                );
        LOGGER.warn(message);
        retryCount.incrementAndGet();
        this.context.timeout(this.configuration.getRetryBackoffTimeMillis());
        throw new RetriableException(message);
      }

      // NOT retrying -> data loss
      final String message = MessageFormat
              .format("Non-retriable Error with last call {} {} {} ",
                      response.getStatusCode(),
                      response.getErrorString(),
                      response.getResponseString()
              );
      LOGGER.error(message);
    }

  }

  @Override
  public void stop() {
    LOGGER.info("stopping lambda connector {} task {}.",
            this.properties.getOrDefault(LambdaSinkConnectorConfig.ConfigurationKeys.NAME_CONFIG.getValue(), "undefined"),
            this.properties.getOrDefault(LambdaSinkConnectorConfig.ConfigurationKeys.TASK_ID.getValue(), "undefined"));
  }

  class LambdaSinkTaskConfiguration extends LambdaSinkConnectorConfig {

    private final String taskId;

    LambdaSinkTaskConfiguration(final Map<String, String> properties) {
      super(LambdaSinkConnectorConfig.getConfigDefinition(), properties);
      this.taskId = "0";//this.getString(ConfigurationKeys.TASK_ID.getValue());
    }

    String getTaskId() {
      return this.taskId;
    }
  }

  private class OutOfRetriesException extends RuntimeException {

    OutOfRetriesException(final String message) {
      super(message);
    }
  }
}
