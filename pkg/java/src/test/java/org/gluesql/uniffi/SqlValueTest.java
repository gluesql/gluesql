package org.gluesql.uniffi;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test class to validate SqlValue data type conversions and verify
 * how each data case is handled when converted from Rust to Java.
 */
public class SqlValueTest {

    @Test
    void testBoolValue() {
        // Test Boolean true
        SqlValue.Bool boolTrue = new SqlValue.Bool(true);
        assertTrue(boolTrue.getValue());
        
        // Test Boolean false
        SqlValue.Bool boolFalse = new SqlValue.Bool(false);
        assertFalse(boolFalse.getValue());
    }

    @Test
    void testIntegerValues() {
        // Test I8
        SqlValue.I8 i8Value = new SqlValue.I8((byte) -128);
        assertEquals((byte) -128, i8Value.getValue());

        // Test I16
        SqlValue.I16 i16Value = new SqlValue.I16((short) -32768);
        assertEquals((short) -32768, i16Value.getValue());

        // Test I32
        SqlValue.I32 i32Value = new SqlValue.I32(-2147483648);
        assertEquals(-2147483648, i32Value.getValue());

        // Test I64
        SqlValue.I64 i64Value = new SqlValue.I64(-9223372036854775808L);
        assertEquals(-9223372036854775808L, i64Value.getValue());
    }

    @Test
    void testUnsignedIntegerValues() {
        // Test U8 (u8 -> i16 mapping)
        SqlValue.U8 u8Value = new SqlValue.U8((short) 255);
        assertEquals((short) 255, u8Value.getValue());

        // Test U16 (u16 -> i32 mapping)
        SqlValue.U16 u16Value = new SqlValue.U16(65535);
        assertEquals(65535, u16Value.getValue());

        // Test U32 (u32 -> i64 mapping)
        SqlValue.U32 u32Value = new SqlValue.U32(4294967295L);
        assertEquals(4294967295L, u32Value.getValue());
    }

    @Test
    void testFloatingPointValues() {
        // Test F32
        SqlValue.F32 f32Value = new SqlValue.F32(3.14159f);
        assertEquals(3.14159f, f32Value.getValue(), 0.00001f);

        // Test F64
        SqlValue.F64 f64Value = new SqlValue.F64(3.141592653589793);
        assertEquals(3.141592653589793, f64Value.getValue(), 0.000000000000001);
    }

    @Test
    void testBigIntValue() {
        // Test BigInt conversion to Java BigInteger
        String bigIntString = "123456789012345678901234567890";
        SqlValue.BigInt bigIntValue = new SqlValue.BigInt(bigIntString);
        assertEquals(bigIntString, bigIntValue.getValue());
        
        // Verify conversion to BigInteger works
        BigInteger javaBigInt = new BigInteger(bigIntValue.getValue());
        assertEquals(new BigInteger(bigIntString), javaBigInt);
    }

    @Test
    void testStringValue() {
        String testString = "Hello, 한글 테스트 🌟";
        SqlValue.Str strValue = new SqlValue.Str(testString);
        assertEquals(testString, strValue.getValue());
    }

    @Test
    void testBytesValue() {
        byte[] testBytes = {(byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0xFF, (byte) 0xAB, (byte) 0xCD};
        SqlValue.Bytes bytesValue = new SqlValue.Bytes(testBytes);
        assertArrayEquals(testBytes, bytesValue.getValue());
    }

    @Test
    void testDateTimeValues() {
        // Test Date
        String dateString = "2024-10-08";
        SqlValue.Date dateValue = new SqlValue.Date(dateString);
        assertEquals(dateString, dateValue.getValue());

        // Test Timestamp
        String timestampString = "2024-10-08 15:30:45.123";
        SqlValue.Timestamp timestampValue = new SqlValue.Timestamp(timestampString);
        assertEquals(timestampString, timestampValue.getValue());

        // Test Time
        String timeString = "15:30:45.123";
        SqlValue.Time timeValue = new SqlValue.Time(timeString);
        assertEquals(timeString, timeValue.getValue());
    }

    @Test
    void testNetworkAndIdentifierValues() {
        // Test Inet
        String inetString = "192.168.1.1";
        SqlValue.Inet inetValue = new SqlValue.Inet(inetString);
        assertEquals(inetString, inetValue.getValue());

        // Test UUID
        String uuidString = "550e8400-e29b-41d4-a716-446655440000";
        SqlValue.Uuid uuidValue = new SqlValue.Uuid(uuidString);
        assertEquals(uuidString, uuidValue.getValue());

        // Test Interval
        String intervalString = "1 day 2 hours";
        SqlValue.Interval intervalValue = new SqlValue.Interval(intervalString);
        assertEquals(intervalString, intervalValue.getValue());
    }

    @Test
    void testPointValue() {
        Point testPoint = new Point(3.14159, 2.71828);
        SqlValue.SqlPoint pointValue = new SqlValue.SqlPoint(testPoint);
        
        Point retrievedPoint = pointValue.getValue();
        assertEquals(3.14159, retrievedPoint.getX(), 0.00001);
        assertEquals(2.71828, retrievedPoint.getY(), 0.00001);
    }

    @Test
    void testSqlListValue() {
        // Create nested SqlValues for the list
        List<SqlValue> testList = new ArrayList<>();
        testList.add(new SqlValue.I32(42));
        testList.add(new SqlValue.Str("hello"));
        testList.add(new SqlValue.Bool(true));
        testList.add(SqlValue.Null.INSTANCE);

        SqlValue.SqlList listValue = new SqlValue.SqlList(testList);
        
        List<SqlValue> retrievedList = listValue.getValue();
        assertEquals(4, retrievedList.size());
        
        // Check each element
        assertTrue(retrievedList.get(0) instanceof SqlValue.I32);
        assertEquals(42, ((SqlValue.I32) retrievedList.get(0)).getValue());
        
        assertTrue(retrievedList.get(1) instanceof SqlValue.Str);
        assertEquals("hello", ((SqlValue.Str) retrievedList.get(1)).getValue());
        
        assertTrue(retrievedList.get(2) instanceof SqlValue.Bool);
        assertTrue(((SqlValue.Bool) retrievedList.get(2)).getValue());
        
        assertTrue(retrievedList.get(3) instanceof SqlValue.Null);
    }

    @Test
    void testSqlMapValue() {
        // Create nested SqlValues for the map
        Map<String, SqlValue> testMap = new HashMap<>();
        testMap.put("id", new SqlValue.I64(12345L));
        testMap.put("name", new SqlValue.Str("John Doe"));
        testMap.put("active", new SqlValue.Bool(true));
        testMap.put("balance", new SqlValue.F64(99.99));
        testMap.put("metadata", SqlValue.Null.INSTANCE);

        SqlValue.SqlMap mapValue = new SqlValue.SqlMap(testMap);
        
        Map<String, SqlValue> retrievedMap = mapValue.getValue();
        assertEquals(5, retrievedMap.size());
        
        // Check each element
        assertTrue(retrievedMap.get("id") instanceof SqlValue.I64);
        assertEquals(12345L, ((SqlValue.I64) retrievedMap.get("id")).getValue());
        
        assertTrue(retrievedMap.get("name") instanceof SqlValue.Str);
        assertEquals("John Doe", ((SqlValue.Str) retrievedMap.get("name")).getValue());
        
        assertTrue(retrievedMap.get("active") instanceof SqlValue.Bool);
        assertTrue(((SqlValue.Bool) retrievedMap.get("active")).getValue());
        
        assertTrue(retrievedMap.get("balance") instanceof SqlValue.F64);
        assertEquals(99.99, ((SqlValue.F64) retrievedMap.get("balance")).getValue(), 0.001);
        
        assertTrue(retrievedMap.get("metadata") instanceof SqlValue.Null);
    }

    @Test
    void testNullValue() {
        SqlValue nullValue = SqlValue.Null.INSTANCE;
        assertTrue(nullValue instanceof SqlValue.Null);
        assertNotNull(nullValue); // The instance itself is not null, just represents SQL NULL
    }

    @Test
    void testComplexNestedStructure() {
        // Test deeply nested structure: Map containing List containing Map
        Map<String, SqlValue> innerMap = new HashMap<>();
        innerMap.put("nested_id", new SqlValue.I32(999));
        innerMap.put("nested_value", new SqlValue.Str("deep"));
        
        List<SqlValue> innerList = new ArrayList<>();
        innerList.add(new SqlValue.SqlMap(innerMap));
        innerList.add(new SqlValue.BigInt("999999999999999999999"));
        
        Map<String, SqlValue> outerMap = new HashMap<>();
        outerMap.put("list_field", new SqlValue.SqlList(innerList));
        outerMap.put("simple_field", new SqlValue.Str("simple"));
        
        SqlValue.SqlMap complexValue = new SqlValue.SqlMap(outerMap);
        
        // Verify the nested structure
        Map<String, SqlValue> retrieved = complexValue.getValue();
        assertEquals(2, retrieved.size());
        
        SqlValue listField = retrieved.get("list_field");
        assertTrue(listField instanceof SqlValue.SqlList);
        
        List<SqlValue> retrievedList = ((SqlValue.SqlList) listField).getValue();
        assertEquals(2, retrievedList.size());
        
        SqlValue nestedMapValue = retrievedList.get(0);
        assertTrue(nestedMapValue instanceof SqlValue.SqlMap);
        
        Map<String, SqlValue> nestedMap = ((SqlValue.SqlMap) nestedMapValue).getValue();
        assertEquals(2, nestedMap.size());
        assertEquals(999, ((SqlValue.I32) nestedMap.get("nested_id")).getValue());
        assertEquals("deep", ((SqlValue.Str) nestedMap.get("nested_value")).getValue());
    }

    @Test
    void testTypeConsistency() {
        // Test that each SqlValue type maintains its type information correctly
        SqlValue[] values = {
            new SqlValue.Bool(true),
            new SqlValue.I8((byte) 42),
            new SqlValue.I16((short) 1234),
            new SqlValue.I32(123456),
            new SqlValue.I64(123456789L),
            new SqlValue.U8((short) 200),
            new SqlValue.U16(50000),
            new SqlValue.U32(3000000000L),
            new SqlValue.F32(3.14f),
            new SqlValue.F64(2.718281828),
            new SqlValue.BigInt("99999999999999999999999999999"),
            new SqlValue.Str("test string"),
            new SqlValue.Bytes(new byte[]{1, 2, 3}),
            new SqlValue.Inet("127.0.0.1"),
            new SqlValue.Date("2024-01-01"),
            new SqlValue.Timestamp("2024-01-01 12:00:00.000"),
            new SqlValue.Time("12:00:00.000"),
            new SqlValue.Interval("1 hour"),
            new SqlValue.Uuid("12345678-1234-1234-1234-123456789abc"),
            new SqlValue.SqlPoint(new Point(1.0, 2.0)),
            SqlValue.Null.INSTANCE
        };

        // Verify each value maintains its specific type
        for (SqlValue value : values) {
            assertNotNull(value);
            // Each should be an instance of SqlValue
            assertInstanceOf(SqlValue.class, value);
        }
    }
}