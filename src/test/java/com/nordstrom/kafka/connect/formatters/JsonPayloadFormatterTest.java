package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class JsonPayloadFormatterTest {
  private Logger log = LoggerFactory.getLogger(this.getClass());

  // So we can output test name for debug logging.
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
  private static String TEST_KEY_JSON = "{\"key_name\" : \"test-key-json\"}";
  private static String TEST_VALUE_CLASS = TestValue.class.getCanonicalName();
  private static String TEST_VALUE = "test-value";
  private static String TEST_VALUE_KEY = "test-value-key";
  private static String TEST_VALUE_JSON = "{\"value_name\" : \"test-value-json\"}";
  private static String TEST_VALUE_LIST = "[" + TEST_VALUE + "]";
  private static String TEST_VALUE_MAP = "{" + TEST_VALUE_KEY + "=" + TEST_VALUE + "}";
  private static String KEY_SCHEMA_VISIBILITY_CONFIG = "key.schema.visibility";
  private static String VALUE_SCHEMA_VISIBILITY_CONFIG = "value.schema.visibility";

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
        .version(1234)
        .build();
    valueSchema = SchemaBuilder.struct()
        .name(TEST_VALUE_CLASS)
        .field("value_name", Schema.STRING_SCHEMA)
        .version(5678)
        .build();
    keyStruct = new Struct(keySchema)
        .put("key_name", "test-key");
    valueStruct = new Struct(valueSchema)
        .put("value_name", TEST_VALUE);
    valueList = new ArrayList<>();
    valueList.add(TEST_VALUE);
    valueMap = new HashMap<>();
    valueMap.put(TEST_VALUE_KEY, TEST_VALUE);

    formatter = new JsonPayloadFormatter();
    formatter.configure(Collections.emptyMap());
    mapper = new ObjectMapper();
  }

  //
  // key/value test combinations.  A key or value can be of type null, string, serialized-json (still a string),
  // integer/long, boolean, and Avro schema.  The naming convention for the test is:
  //    test<key-type><value-type>SinkRecord.
  //
  @Test
  public void testAvroAvroSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    // schema.visibility = 'min' is default for both key and value.
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    assertTrue(payload.getKey() instanceof HashMap);
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(1234, Integer.parseInt(payload.getKeySchemaVersion()));
    assertTrue(payload.getValue() instanceof HashMap);
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
    assertEquals(5678, Integer.parseInt(payload.getValueSchemaVersion()));
  }

  @Test
  public void testAvroAvroSinkRecordKeySchemaVisibilityNone() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    Map<String, String> map = new HashMap<>();
    map.put(KEY_SCHEMA_VISIBILITY_CONFIG, "none");
    formatter.configure(map);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    final Map<String, String> key = new HashMap<>();
    key.put("key_name", "test-key");
    assertEquals(key, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertNull(payload.getKeySchemaVersion());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
    assertEquals(5678, Integer.parseInt(payload.getValueSchemaVersion()));
  }

  @Test
  public void testAvroAvroSinkRecordKeySchemaVisibilityAll() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    Map<String, String> map = new HashMap<>();
    map.put(KEY_SCHEMA_VISIBILITY_CONFIG, "all");
    formatter.configure(map);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    assertTrue(payload.getKey() instanceof HashMap);
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(1234, Integer.parseInt(payload.getKeySchemaVersion()));
    assertTrue(payload.getValue() instanceof HashMap);
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
    assertEquals(5678, Integer.parseInt(payload.getValueSchemaVersion()));
  }

  @Test
  public void testAvroAvroSinkRecordValueSchemaVisibilityNone() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    Map<String, String> map = new HashMap<>();
    map.put(VALUE_SCHEMA_VISIBILITY_CONFIG, "none");
    formatter.configure(map);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    assertTrue(payload.getKey() instanceof HashMap);
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(1234, Integer.parseInt(payload.getKeySchemaVersion()));
    assertNull(payload.getValueSchemaName());
    assertNull(payload.getValueSchemaVersion());
  }

  @Test
  public void testAvroAvroSinkRecordValueSchemaVisibilityAll() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    Map<String, String> map = new HashMap<>();
    map.put(VALUE_SCHEMA_VISIBILITY_CONFIG, "all");
    formatter.configure(map);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    // Only asserting on things that are addressed by the test.
    assertTrue(payload.getKey() instanceof HashMap);
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(1234, Integer.parseInt(payload.getKeySchemaVersion()));
    assertTrue(payload.getValue() instanceof HashMap);
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
    assertEquals(5678, Integer.parseInt(payload.getValueSchemaVersion()));
  }

  @Test
  public void testAvroJsonListSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, valueList);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertTrue(payload.getKey() instanceof HashMap);
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

    assertTrue(payload.getKey() instanceof HashMap);
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

    assertTrue(payload.getKey() instanceof HashMap);
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
    assertTrue(payload.getValue() instanceof HashMap);
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
    final SinkRecord record = createSinkRecord(null, "test-key", valueSchema, valueStruct);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals("test-key", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertTrue(payload.getValue() instanceof HashMap);
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testStringJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, "test-key", null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals("test-key", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testIntegerJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, 123, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(123, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testLongJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, 123L, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(123L, Long.parseLong(payload.getKey().toString()));
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testFloatJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, 123.45, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals(123.45, Float.parseFloat(payload.getKey().toString()), 0.0001);
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testBooleanJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, true, null, TEST_VALUE_JSON);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertTrue(Boolean.parseBoolean(payload.getKey().toString()));
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, "test-key", null, TEST_VALUE);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals("test-key", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertNull(payload.getKeySchemaVersion());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
    assertNull(payload.getValueSchemaVersion());
  }

  @Test
  public void testStringStringSinkRecordKeyValueSchemaNone() throws IOException {
    // key and value schema should have no affect on plain string values.
    final SinkRecord record = createSinkRecord(null, "test-key", null, TEST_VALUE);
    Map<String, String> map = new HashMap<>();
    map.put(KEY_SCHEMA_VISIBILITY_CONFIG, "none");
    map.put(VALUE_SCHEMA_VISIBILITY_CONFIG, "none");
    formatter.configure(map);
    final String result = formatter.format(record);
    debugShow(record, result);

    Payload payload = new Payload<>();
    payload = mapper.readValue(result, payload.getClass());

    assertEquals("test-key", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertNull(payload.getKeySchemaVersion());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
    assertNull(payload.getValueSchemaVersion());
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
    assertTrue(payload.getValue() instanceof HashMap);
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
      log.debug("\n---[ {}.{} ]---\n{}\n", this.getClass().getSimpleName(), tname.getMethodName(), result);

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
  public void testFormatBatchOfRecords() throws IOException {
    List<SinkRecord> records = Arrays.asList(
        createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct),
        createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct)
    );
    final String result = formatter.format(records);
    debugShow(records, result);

    Payload[] payloads = mapper
        .readValue(result, Payload[].class);

    assertEquals(records.size(), payloads.length);
    for (Payload payload : payloads) {
      assertTrue(payload.getKey() instanceof HashMap);
      assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
      assertEquals(1234, Integer.parseInt(payload.getKeySchemaVersion()));
      assertTrue(payload.getValue() instanceof HashMap);
      assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
      assertEquals(5678, Integer.parseInt(payload.getValueSchemaVersion()));
    }
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
    log.debug("\n===[ {}.{} ]===\nrecord={}\n-----\nresult={}\n",
        this.getClass().getSimpleName(), tname.getMethodName(),
        record, result);
  }

  private void debugShow(List<SinkRecord> records, String result) {
    log.debug("\n===[ {}.{} ]===", this.getClass().getSimpleName(), tname.getMethodName());
    for (SinkRecord record : records) {
      log.debug(record.toString());
    }
    log.debug("\n-----\nresult={}\n", result);
  }

} //-JsonPayloadFormatterTest
