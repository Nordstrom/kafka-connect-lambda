package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Collections.emptyMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JsonPayloadFormatterTest {
  @Rule
  public TestName tname = new TestName();

  private Schema keySchema;
  private Struct keyStruct;
  private List<String> valueList;
  private Map<String, String> valueMap;
  private Schema valueSchema;
  private Struct valueStruct;
  private JsonPayloadFormatter formatter;
  private ObjectMapper mapper;

  private static String TEST_TOPIC = "test-topic";
  private static int TEST_PARTITION = 1;
  private static long TEST_OFFSET = 2L;
  private static long TEST_TIMESTAMP = 3L;
  private static String TEST_KEY_CLASS = TestKey.class.getCanonicalName();
  private static String TEST_KEY_FIELD = "key_name";
  private static String TEST_KEY_VALUE = "test-key";
  private static String TEST_KEY_JSON = "{\"key_name\" : \"test-key-json\"}";
  private static Integer TEST_KEY_VERSION = 1234;
  private static String TEST_VALUE_CLASS = TestValue.class.getCanonicalName();
  private static String TEST_VALUE_FIELD = "value_name";
  private static String TEST_VALUE = "test-value";
  private static String TEST_VALUE_KEY = "test-value-key";
  private static String TEST_VALUE_JSON = "{\"value_name\" : \"test-value-json\"}";
  private static Integer TEST_VALUE_VERSION = 5678;
  private static String TEST_VALUE_LIST = "[" + TEST_VALUE + "]";
  private static String TEST_VALUE_MAP = "{" + TEST_VALUE_KEY + "=" + TEST_VALUE + "}";//"{value_name=test-value}"

  private static class TestKey {
    public String key_name;

    public TestKey() {
    }
  }

  private static class TestValue {
    public String value_name;

    public TestValue() {
    }
  }

  @Before
  public void setup() {
    keySchema = SchemaBuilder.struct()
        .name(TEST_KEY_CLASS)
        .field("key_name", Schema.STRING_SCHEMA)
        .version(TEST_KEY_VERSION)
        .build();
    valueSchema = SchemaBuilder.struct()
        .name(TEST_VALUE_CLASS)
        .field(TEST_VALUE_FIELD, Schema.STRING_SCHEMA)
        .version(TEST_VALUE_VERSION)
        .build();
    keyStruct = new Struct(keySchema)
        .put(TEST_KEY_FIELD, TEST_KEY_VALUE);
    valueStruct = new Struct(valueSchema)
        .put(TEST_VALUE_FIELD, TEST_VALUE);
    valueList = new ArrayList<>();
    valueList.add(TEST_VALUE);
    valueMap = new HashMap<>();
    valueMap.put(TEST_VALUE_KEY, TEST_VALUE);

    formatter = new JsonPayloadFormatter();
    mapper = new ObjectMapper();
  }

  //
  // key/value combinations
  //
  @Test
  public void testAvroAvroSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
//    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_KEY_VERSION.toString(), payload.getKeySchemaVersion());
//    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
    assertEquals(TEST_VALUE_VERSION.toString(), payload.getValueSchemaVersion());
  }

  @Test
  public void testAvroAvroSinkRecordNoSchema() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    Map<String,String> formatterConfig = new HashMap<>();
    formatterConfig.put("formatter.schemas.enable", "false");
    formatter.configure(formatterConfig);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    final Map<String, String> key = new HashMap<>();
    key.put(TEST_KEY_FIELD, TEST_KEY_VALUE);
    final Map<String, String> value = new HashMap<>();
    value.put(TEST_VALUE_FIELD, TEST_VALUE);
    assertEquals(key, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertNull(payload.getKeySchemaVersion());
    assertEquals(value, payload.getValue());
    assertNull(payload.getValueSchemaName());
    assertNull(payload.getValueSchemaVersion());
  }


  @Test
  public void testAvroJsonListSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, valueList);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

//    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE_LIST, payload.getValue().toString());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAvroJsonMapSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, valueMap);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

//    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE_MAP, payload.getValue().toString());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAvroStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, TEST_VALUE);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

//    assertEquals(keyStruct.toString(), payload.getKey()); //TODO
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testJsonAvroSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, valueSchema, valueStruct);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
//    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testJsonJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testJsonStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, null, TEST_VALUE);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringAvroSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_VALUE, valueSchema, valueStruct);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_VALUE, payload.getKey());
    assertNull(payload.getKeySchemaName());
//    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testStringJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_VALUE, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_VALUE, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_VALUE, null, TEST_VALUE);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_VALUE, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringStringSinkRecordNoSchema() throws IOException {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_VALUE, null, TEST_VALUE);
    Map<String,String> formatterConfig = new HashMap<>();
    formatterConfig.put("formatter.schemas.enable", "false");
    formatter.configure(formatterConfig);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(TEST_KEY_VALUE, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullAvroSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, valueSchema, valueStruct);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertNull(payload.getKey());
    assertNull(payload.getKeySchemaName());
//    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testNullJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertNull(payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, TEST_VALUE);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertNull(payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullNullSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, null);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertNull(payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertNull(payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAllSchemaTypesSinkRecord() throws IOException {
    class SchemaObjectTest {
      private final Schema schema;
      private final Object object;
      private final String expected;

      private SchemaObjectTest(Schema s, Object o, String e) {
        schema = s;
        object = o;
        expected = e;
      }
    }
    final ArrayList<SchemaObjectTest> schemaObjectTests = new ArrayList<>();
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.array(keySchema).build(), keySchema, keySchema.toString()));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.bool().build(), Boolean.TRUE, "{schema={type=boolean, optional=false}, payload=true}"));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.bytes().build(), "xyzzy".getBytes(), "xyzzy"));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.float32().build(), Float.MAX_VALUE, Float.toString((Float.MAX_VALUE))));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.float64().build(), Float.MAX_VALUE, Float.toString((Float.MAX_VALUE))));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int8().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int16().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int32().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int64().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.map(keySchema, keySchema).build(), keySchema, keySchema.toString()));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.string().build(), "xyzzy", "xyzzy"));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.struct().build(), keyStruct, keyStruct.toString()));
    for (SchemaObjectTest t : schemaObjectTests) {
      final SinkRecord record = createSinkRecord(t.schema, t.object, t.schema, t.object);
      final String result = formatter.format(record);
      final String s = MessageFormat.format("\n---[ {0}.{1} ]---\n{2}", this.getClass().getSimpleName(), tname.getMethodName(), result);
      System.out.println(s);

      Payload payload = new Payload<>();
      payload = mapper.readValue(result, payload.getClass());

      assertEquals(TEST_TOPIC, payload.getTopic());
      assertEquals(TEST_PARTITION, payload.getPartition());
      assertNull(payload.getKeySchemaName());
//      assertEquals(t.expected, payload.getKey());
      assertNull(payload.getValueSchemaName());
//      assertEquals(t.expected, payload.getValue());
      assertEquals(TEST_OFFSET, payload.getOffset());
      assertEquals(TEST_TIMESTAMP, payload.getTimestamp());
      assertEquals(TimestampType.NO_TIMESTAMP_TYPE.toString(), payload.getTimestampTypeName());
    }
  }

  @Test
  public void testTimestampTypesSinkRecord() throws IOException {
    TimestampType[] timestampTypes = {
        TimestampType.LOG_APPEND_TIME,
        TimestampType.CREATE_TIME,
        TimestampType.NO_TIMESTAMP_TYPE
    };

    for (TimestampType t : timestampTypes) {
      final SinkRecord record = new SinkRecord(
          TEST_TOPIC,
          TEST_PARTITION,
          null,
          null,
          null,
          null,
          TEST_OFFSET,
          TEST_TIMESTAMP,
          t
      );
      final String result = formatter.format(record);
      debugShow(record, result);

      Payload payload = new Payload<>();
      payload = mapper.readValue(result, payload.getClass());
      assertEquals(t.toString(), payload.getTimestampTypeName());
    }
  }

  @Test
  @Ignore("TODO")
  public void testFormatBatchOfRecords() throws IOException {
  }


  //
  // helpers
  //
  private SinkRecord createSinkRecord(Schema keySchema, Object key, Schema valueSchema, Object value) {
    return new SinkRecord(
        TEST_TOPIC,
        TEST_PARTITION,
        keySchema,
        key,
        valueSchema,
        value,
        TEST_OFFSET,
        TEST_TIMESTAMP,
        TimestampType.NO_TIMESTAMP_TYPE
    );
  }

  private void debugShow(SinkRecord record, String result) {
    final String s = MessageFormat.format("\n===[ {0}.{1} ]===\n{2}\n-----\n{3}",
        this.getClass().getSimpleName(), tname.getMethodName(),
        record, result);
    System.out.println(s);
  }

} //-JsonPayloadFormatterTest
