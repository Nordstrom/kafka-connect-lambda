package com.nordstrom.kafka.connect.lambda;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.nordstrom.kafka.connect.About;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LambdaSinkConnector extends SinkConnector {
  private static Logger LOGGER = LoggerFactory.getLogger(LambdaSinkConnector.class);
  private LambdaSinkConnectorConfig configuration;

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks)
            .mapToObj(i -> {
              final Map<String, String> taskProperties = new HashMap<>(
                      this.configuration.getProperties());
              return taskProperties;
            })
            .collect(Collectors.toList());
  }

  @Override
  public void start(Map<String, String> settings) {
    LOGGER.info("starting connector {} with properties {}",
            settings.getOrDefault(LambdaSinkConnectorConfig.ConfigurationKeys.NAME_CONFIG.getValue(), ""),
            settings);

    this.configuration = new LambdaSinkConnectorConfig(settings);

    LOGGER.info("connector.start:OK");
  }

  @Override
  public void stop() {
    LOGGER.info("connector.stop:OK");
  }

  @Override
  public ConfigDef config() {
    return LambdaSinkConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return LambdaSinkTask.class;
  }

  @Override
  public String version() {
    return About.CURRENT_VERSION;
  }
}
