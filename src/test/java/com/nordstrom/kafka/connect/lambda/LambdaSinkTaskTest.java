package com.nordstrom.kafka.connect.lambda;

import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LambdaSinkTaskTest {
    @Ignore("Test is ignored as a demonstration -- needs credentials")
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

        InvocationClient mockedClient = mock(InvocationClient.class);

        when(mockedClient.invoke(any()))
            .thenReturn(new InvocationResponse(200, "test log", "", Instant.now(), Instant.now()));

        Schema testSchema = SchemaBuilder.struct().name("com.nordstrom.kafka.connect.lambda.foo").field("bar", STRING_SCHEMA).build();

        SinkRecord testRecord = new SinkRecord("connect-lambda-test", 0, STRING_SCHEMA, "sometestkey", testSchema, "testing", 0, 0L, TimestampType.CREATE_TIME);
        Collection<SinkRecord> testList = new ArrayList<>();
        testList.add(testRecord);

        task.put(testList);
    }
}
