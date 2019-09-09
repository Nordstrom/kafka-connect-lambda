package com.nordstrom.kafka.connect.formatters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PlainPayloadFormatterTest {
  private Logger log = LoggerFactory.getLogger(this.getClass());

  // So we can output test name for debug logging.
  @Rule
  public TestName tname = new TestName();

  private List<String> keyList;
  private Map<String, String> keyMap;
  private Schema keySchema;
  private Struct keyStruct;
  private List<String> valueList;
  private Map<String, String> valueMap;
  private Schema valueSchema;
  private Struct valueStruct;
  private PlainPayloadFormatter formatter;
  private ObjectMapper mapper;

  private static String TEST_TOPIC = "test-topic";
  private static int TEST_PARTITION = 1;
  private static long TEST_OFFSET = 2L;
  private static long TEST_TIMESTAMP = 3L;
  private static String TEST_KEY_CLASS = TestKey.class.getCanonicalName();
  private static String TEST_KEY = "test-key";
  private static String TEST_KEY_JSON = "{\"key_name\" : \"test-key-json\"}";
  private static String TEST_VALUE_CLASS = TestValue.class.getCanonicalName();
  private static String TEST_VALUE_FIELD = "value_name";
  private static String TEST_VALUE_KEY = "test-value-key";
  private static String TEST_VALUE = "test-value";
  private static String TEST_VALUE_JSON = "{\"value_name\" : \"test-value-json\"}";
  private static String TEST_VALUE_LIST = "[" + TEST_VALUE + "]";
  private static String TEST_VALUE_MAP = "{" + TEST_VALUE_KEY + "=" + TEST_VALUE + "}";//"{value_name=test-value}"

  private static class TestKey {
    public String key;

    public TestKey() {
    }
  }

  private static class TestValue {
    public String value;

    public TestValue() {
    }
  }

  @Before
  public void setup() {
    keySchema = SchemaBuilder.struct()
        .name(TEST_KEY_CLASS)
        .field("key_name", Schema.STRING_SCHEMA)
        .build();
    valueSchema = SchemaBuilder.struct()
        .name(TEST_VALUE_CLASS)
        .field(TEST_VALUE_FIELD, Schema.STRING_SCHEMA)
        .build();
    keyStruct = new Struct(keySchema)
        .put("key_name", TEST_KEY);
    valueStruct = new Struct(valueSchema)
        .put(TEST_VALUE_FIELD, TEST_VALUE);
    keyList = new ArrayList<>();
    keyList.add(TEST_KEY);
    keyMap = new HashMap<>();
    keyMap.put(TEST_KEY, TEST_VALUE);
    valueList = new ArrayList<>();
    valueList.add(TEST_VALUE);
    valueMap = new HashMap<>();
    valueMap.put(TEST_VALUE_KEY, TEST_VALUE);

    formatter = new PlainPayloadFormatter();
    mapper = new ObjectMapper();
  }

  @Test
  public void testFormatBatchOfRecords() throws IOException {
    List<SinkRecord> records = Arrays.asList(
        new SinkRecord("test-topic", 1, null, "test-key1", null, "test-value1", 0),
        new SinkRecord("test-topic", 1, null, "test-key2", null, "test-value2", 1),
        new SinkRecord("test-topic", 1, null, "test-key3", null, "test-value3", 2)
    );

    String result = formatter.format(records);

    PlainPayload[] payloads = mapper
        .readValue(result, PlainPayload[].class);

    assertEquals(3, payloads.length);
    for (int i = 0; i < payloads.length; i++) {
      assertEquals(i, payloads[i].getOffset());
    }
  }

  //
  // key/value combinations
  //
  @Test
  public void testAvroAvroSinkRecord() {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, valueSchema, valueStruct);
    final PlainPayload payload = derivePayload(record);

    // Only asserting on things that are address by the test.
    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testAvroJsonListSinkRecord() {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, valueList);
    final PlainPayload payload = derivePayload(record);

    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE_LIST, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAvroJsonMapSinkRecord() {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, valueMap);
    final PlainPayload payload = derivePayload(record);

    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE_MAP, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAvroStringSinkRecord() {
    final SinkRecord record = createSinkRecord(keySchema, keyStruct, null, TEST_VALUE);
    final PlainPayload payload = derivePayload(record);

    assertEquals(keyStruct.toString(), payload.getKey());
    assertEquals(TEST_KEY_CLASS, payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testJsonAvroSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, valueSchema, valueStruct);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testJsonJsonSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, null, TEST_VALUE_JSON);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testJsonStringSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY_JSON, null, TEST_VALUE);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY_JSON, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringAvroSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY, valueSchema, valueStruct);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testStringJsonSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY, null, TEST_VALUE_JSON);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testStringStringSinkRecord() {
    final SinkRecord record = createSinkRecord(null, TEST_KEY, null, TEST_VALUE);
    final PlainPayload payload = derivePayload(record);

    assertEquals(TEST_KEY, payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullAvroSinkRecord() {
    final SinkRecord record = createSinkRecord(null, null, valueSchema, valueStruct);
    final PlainPayload payload = derivePayload(record);

    assertEquals("", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(valueStruct.toString(), payload.getValue());
    assertEquals(TEST_VALUE_CLASS, payload.getValueSchemaName());
  }

  @Test
  public void testNullJsonSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, TEST_VALUE_JSON);
    final PlainPayload payload = derivePayload(record);

    assertEquals("", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE_JSON, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullStringSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, TEST_VALUE);
    final PlainPayload payload = derivePayload(record);

    assertEquals("", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals(TEST_VALUE, payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testNullNullSinkRecord() throws IOException {
    final SinkRecord record = createSinkRecord(null, null, null, null);
    final PlainPayload payload = derivePayload(record);

    assertEquals("", payload.getKey());
    assertNull(payload.getKeySchemaName());
    assertEquals("", payload.getValue());
    assertNull(payload.getValueSchemaName());
  }

  @Test
  public void testAllSchemaTypesSinkRecord() throws IOException, IllegalAccessException, InstantiationException {
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
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.array(keySchema).build(), keySchema, keySchema.toString()));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.bool().build(), Boolean.TRUE, "true"));
//    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.bytes().build(), "xyzzy".getBytes(), "xyzzy")); // TODO
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.float32().build(), Float.MAX_VALUE, Float.toString((Float.MAX_VALUE))));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.float64().build(), Float.MAX_VALUE, Float.toString((Float.MAX_VALUE))));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int8().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int16().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int32().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.int64().build(), Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.map(keySchema, keySchema).build(), keySchema, keySchema.toString()));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.string().build(), "xyzzy", "xyzzy"));
    schemaObjectTests.add(new SchemaObjectTest(SchemaBuilder.struct().build(), keyStruct, keyStruct.toString()));
    for (SchemaObjectTest t : schemaObjectTests) {
      final SinkRecord record = createSinkRecord(t.schema, t.object, t.schema, t.object);
      final String result = formatter.format(record);
      log.debug("\n---[ {}.{} ]---\n{}\n", this.getClass().getSimpleName(), tname.getMethodName(), result);
      final PlainPayload payload = mapper
          .readValue(result, PlainPayload.class);

      assertEquals(TEST_TOPIC, payload.getTopic());
      assertEquals(TEST_PARTITION, payload.getPartition());
      assertNull(payload.getKeySchemaName());
      assertEquals(t.expected, payload.getKey());
      assertNull(payload.getValueSchemaName());
      assertEquals(t.expected, payload.getValue());
      assertEquals(TEST_OFFSET, payload.getOffset());
      assertEquals(new Long(TEST_TIMESTAMP), payload.getTimestamp());
      assertEquals(TimestampType.NO_TIMESTAMP_TYPE.toString(), payload.getTimestampTypeName());
    }
  }

  @Test
  public void testTimestampTypesSinkRecord() throws IOException {
    SinkRecord record;
    PlainPayload payload;
    TimestampType[] timestampTypes = {
        TimestampType.LOG_APPEND_TIME,
        TimestampType.CREATE_TIME,
        TimestampType.NO_TIMESTAMP_TYPE
    };

    for (TimestampType t : timestampTypes) {
      record = new SinkRecord(
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
      payload = derivePayload(record);
      assertEquals(t.toString(), payload.getTimestampTypeName());
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

  private PlainPayload derivePayload(SinkRecord record) {
    try {
      final String result = formatter.format(record);
      log.debug("\n---[ {}.{} ]---\n{}\n", this.getClass().getSimpleName(), tname.getMethodName(), result);
      return mapper.readValue(result, PlainPayload.class);
    } catch (Exception e) {
      // Convert to unchecked exception.
      throw new RuntimeException(e);
    }
  }

}
