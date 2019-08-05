package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.services.lambda.model.InvocationType;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LambdaSinkTaskTest {

    @Test
    public void testStartProperlyInitializesSinkTaskWithSampleConnectorConfigurations(){

        ImmutableMap<String, String> props =
        new ImmutableMap.Builder<String, String>()
                .put("connector.class", "com.nordstrom.kafka.connect.lambda.LambdaSinkConnector")
                .put("tasks.max", "1")
                .put("aws.region", "test-region")
                .put("aws.lambda.function.arn", "arn:aws:lambda:us-west-2:123456789123:function:test-lambda")
                .put("aws.lambda.invocation.timeout.ms", "300000")
                .put("aws.lambda.invocation.mode", "SYNC")
                .put("aws.lambda.batch.enabled", "true")
                .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .put("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .put("topics", "connect-lambda-test")
                .build();

        LambdaSinkTask task = new LambdaSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        task.start(props);

        assertNotNull(task.configuration);
        assertNotNull(task.properties);
        assertNotNull(task.lambdaClient);

        assertEquals("0", task.configuration.getTaskId());
        assertEquals("test-region", task.configuration.getAwsRegion());
        assertEquals("arn:aws:lambda:us-west-2:123456789123:function:test-lambda", task.configuration.getAwsFunctionArn());
        assertEquals("PT5M", task.configuration.getInvocationTimeout().toString());
        assertEquals("SYNC", task.configuration.getInvocationMode().toString());
        assertTrue(task.configuration.isBatchingEnabled());
        assertEquals(6291455, task.configuration.getMaxBatchSizeBytes());
    }

    @Ignore("Test is ignored as a demonstration -- needs profile")
    @Test
    public void testPutWhenBatchingIsNotEnabled() {

        ImmutableMap<String, String> props =
                new ImmutableMap.Builder<String, String>()
                        .put("connector.class", "com.nordstrom.kafka.connect.lambda.LambdaSinkConnector")
                        .put("tasks.max", "1")
                        .put("aws.lambda.function.arn", "arn:aws:lambda:us-west-2:123456789123:function:test-lambda")
                        .put("aws.lambda.invocation.timeout.ms", "300000")
                        .put("aws.lambda.invocation.mode", "SYNC")
                        .put("aws.lambda.batch.enabled", "false")
                        .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .put("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .put("topics", "connect-lambda-test")
                        .build();

        LambdaSinkTask task = new LambdaSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        task.start(props);

        assertFalse(task.configuration.isBatchingEnabled());

        AwsLambdaUtil mockedLambdaClient = mock(AwsLambdaUtil.class);

        when(mockedLambdaClient.invoke(anyString(), anyObject(), anyObject(), eq(InvocationType.RequestResponse))).thenReturn(new AwsLambdaUtil( new Configuration(
                task.configuration.getHttpProxyHost(),
                task.configuration.getHttpProxyPort(),
                task.configuration.getAwsRegion(),
                task.configuration.getFailureMode(),
                task.configuration.getRoleArn(),
                task.configuration.getSessionName(),
                task.configuration.getExternalId()), new HashMap<>()).new InvocationResponse(200, "test log", "", Instant.now(), Instant.now()));

        Schema testSchema = SchemaBuilder.struct().name("com.nordstrom.kafka.connect.lambda.foo").field("bar", STRING_SCHEMA).build();

        SinkRecord testRecord = new SinkRecord("connect-lambda-test", 0, STRING_SCHEMA, "sometestkey", testSchema, "testing", 0, 0L, TimestampType.CREATE_TIME);
        Collection<SinkRecord> testList = new ArrayList<>();
        testList.add(testRecord);

        task.put(testList);
    }
}
