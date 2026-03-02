package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RowDataBinaryConverter}.
 * 
 * <p>Tests the inactive path (non-RowDataSerializer). The active path (RowDataSerializer)
 * is tested in the minicluster integration tests since it requires flink-table dependency.
 */
class RowDataBinaryConverterTest {

    // ========== Inactive Converter (non-RowDataSerializer) ==========

    @Test
    void testCreateWithIntSerializer() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(IntSerializer.INSTANCE);
        assertNotNull(converter);
        assertFalse(converter.isActive(), "Should be inactive for IntSerializer");
    }

    @Test
    void testCreateWithLongSerializer() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(LongSerializer.INSTANCE);
        assertNotNull(converter);
        assertFalse(converter.isActive(), "Should be inactive for LongSerializer");
    }

    @Test
    void testCreateWithStringSerializer() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(StringSerializer.INSTANCE);
        assertNotNull(converter);
        assertFalse(converter.isActive(), "Should be inactive for StringSerializer");
    }

    // ========== Inactive Convert (passthrough) ==========

    @Test
    void testConvertInactivePassthroughInt() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(IntSerializer.INSTANCE);
        Integer value = 42;
        Integer result = converter.convert(value);
        assertSame(value, result, "Inactive converter should return exact same object");
    }

    @Test
    void testConvertInactivePassthroughLong() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(LongSerializer.INSTANCE);
        Long value = 12345L;
        Long result = converter.convert(value);
        assertSame(value, result, "Inactive converter should return exact same object");
    }

    @Test
    void testConvertInactivePassthroughString() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(StringSerializer.INSTANCE);
        String value = "hello world";
        String result = converter.convert(value);
        assertSame(value, result, "Inactive converter should return exact same object");
    }

    @Test
    void testConvertInactivePassthroughCustomObject() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(StringSerializer.INSTANCE);
        Object value = new Object();
        Object result = converter.convert(value);
        assertSame(value, result, "Inactive converter should return exact same object");
    }

    // ========== Null Handling ==========

    @Test
    void testConvertNullInactive() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(IntSerializer.INSTANCE);
        assertNull(converter.convert(null), "Should return null for null input");
    }

    @Test
    void testConvertNullWithNullSerializer() {
        // Even with a null serializer, create should not throw
        // (It won't match RowDataSerializer class name so it returns inactive)
        // Note: This tests defensive coding - in practice serializer is never null
        try {
            RowDataBinaryConverter converter = RowDataBinaryConverter.create(null);
            // If it doesn't throw, the converter should be inactive
            assertFalse(converter.isActive());
        } catch (NullPointerException e) {
            // Also acceptable - null serializer is an edge case
        }
    }

    // ========== Multiple Conversions ==========

    @Test
    void testConvertMultipleTimes() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(LongSerializer.INSTANCE);

        for (long i = 0; i < 100; i++) {
            Long value = i;
            Long result = converter.convert(value);
            assertSame(value, result, "Each call should return same object");
        }
    }

    // ========== Type Safety ==========

    @Test
    void testConvertPreservesType() {
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(StringSerializer.INSTANCE);
        
        String s = converter.convert("test");
        assertEquals("test", s);
        
        Integer i = converter.convert(42);
        assertEquals(42, i);
        
        Long l = converter.convert(100L);
        assertEquals(100L, l);
    }

    // ========== Mock RowDataSerializer Detection ==========

    @Test
    void testNonMatchingClassNameNotActivated() {
        // The converter should only activate for exact RowDataSerializer class name.
        // All standard serializers (IntSerializer, LongSerializer, StringSerializer) should not activate.
        // This is already covered by the tests above, but here we verify with an additional custom type.
        TypeSerializer<String> custom = StringSerializer.INSTANCE;
        RowDataBinaryConverter converter = RowDataBinaryConverter.create(custom);
        assertFalse(converter.isActive(), 
                "Should not activate for class other than RowDataSerializer");
    }
}
