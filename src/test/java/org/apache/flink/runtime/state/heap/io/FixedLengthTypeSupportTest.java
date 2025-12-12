package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FixedLengthTypeSupport.
 */
class FixedLengthTypeSupportTest {

    @Nested
    class TypeDetectionTests {

        @Test
        void testDetectLongSerializer() {
            TypeSerializer<Long> serializer = LongSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNotNull(info);
            assertEquals(FixedLengthTypeSupport.FixedType.LONG, info.getType());
            assertEquals(8, info.getByteSize());
        }

        @Test
        void testDetectIntSerializer() {
            TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNotNull(info);
            assertEquals(FixedLengthTypeSupport.FixedType.INT, info.getType());
            assertEquals(4, info.getByteSize());
        }

        @Test
        void testDetectDoubleSerializer() {
            TypeSerializer<Double> serializer = DoubleSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNotNull(info);
            assertEquals(FixedLengthTypeSupport.FixedType.DOUBLE, info.getType());
            assertEquals(8, info.getByteSize());
        }

        @Test
        void testDetectFloatSerializer() {
            TypeSerializer<Float> serializer = FloatSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNotNull(info);
            assertEquals(FixedLengthTypeSupport.FixedType.FLOAT, info.getType());
            assertEquals(4, info.getByteSize());
        }

        @Test
        void testDetectBooleanSerializer() {
            TypeSerializer<Boolean> serializer = BooleanSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNotNull(info);
            assertEquals(FixedLengthTypeSupport.FixedType.BOOLEAN, info.getType());
            assertEquals(1, info.getByteSize());
        }

        @Test
        void testDetectStringSerializerReturnsNull() {
            TypeSerializer<String> serializer = StringSerializer.INSTANCE;
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(serializer);
            
            assertNull(info, "StringSerializer is not a fixed-length type");
        }

        @Test
        void testDetectNullReturnsNull() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(null);
            assertNull(info);
        }

        @Test
        void testIsFixedLengthType() {
            assertTrue(FixedLengthTypeSupport.isFixedLengthType(LongSerializer.INSTANCE));
            assertTrue(FixedLengthTypeSupport.isFixedLengthType(IntSerializer.INSTANCE));
            assertTrue(FixedLengthTypeSupport.isFixedLengthType(DoubleSerializer.INSTANCE));
            assertFalse(FixedLengthTypeSupport.isFixedLengthType(StringSerializer.INSTANCE));
            assertFalse(FixedLengthTypeSupport.isFixedLengthType(null));
        }
    }

    @Nested
    class MemoryOperationsTests {

        @Test
        void testLongReadWrite() {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(16);
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(LongSerializer.INSTANCE);
            assertNotNull(info);
            
            long testValue = 0x123456789ABCDEF0L;
            info.write(segment, 0, testValue);
            Long readValue = info.read(segment, 0);
            
            assertEquals(testValue, readValue);
        }

        @Test
        void testIntReadWrite() {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(16);
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(IntSerializer.INSTANCE);
            assertNotNull(info);
            
            int testValue = 0x12345678;
            info.write(segment, 0, testValue);
            Integer readValue = info.read(segment, 0);
            
            assertEquals(testValue, readValue);
        }

        @Test
        void testDoubleReadWrite() {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(16);
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(DoubleSerializer.INSTANCE);
            assertNotNull(info);
            
            double testValue = 3.14159265358979;
            info.write(segment, 0, testValue);
            Double readValue = info.read(segment, 0);
            
            assertEquals(testValue, readValue, 0.0001);
        }

        @Test
        void testBooleanReadWrite() {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(16);
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(BooleanSerializer.INSTANCE);
            assertNotNull(info);
            
            info.write(segment, 0, true);
            assertTrue(info.<Boolean>read(segment, 0));
            
            info.write(segment, 0, false);
            assertFalse(info.<Boolean>read(segment, 0));
        }
    }

    @Nested
    class ByteArrayOperationsTests {

        @Test
        void testLongToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(LongSerializer.INSTANCE);
            assertNotNull(info);
            
            long testValue = 0xFEDCBA9876543210L;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(8, bytes.length);
            
            Long recovered = info.fromBytes(bytes);
            assertEquals(testValue, recovered);
        }

        @Test
        void testIntToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(IntSerializer.INSTANCE);
            assertNotNull(info);
            
            int testValue = 0xDEADBEEF;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(4, bytes.length);
            
            Integer recovered = info.fromBytes(bytes);
            assertEquals(testValue, (int) recovered);
        }

        @Test
        void testDoubleToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(DoubleSerializer.INSTANCE);
            assertNotNull(info);
            
            double testValue = -273.15;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(8, bytes.length);
            
            Double recovered = info.fromBytes(bytes);
            assertEquals(testValue, recovered, 0.0001);
        }

        @Test
        void testFloatToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(FloatSerializer.INSTANCE);
            assertNotNull(info);
            
            float testValue = 2.71828f;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(4, bytes.length);
            
            Float recovered = info.fromBytes(bytes);
            assertEquals(testValue, recovered, 0.0001f);
        }

        @Test
        void testShortToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(ShortSerializer.INSTANCE);
            assertNotNull(info);
            
            short testValue = (short) 0x7FFF;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(2, bytes.length);
            
            Short recovered = info.fromBytes(bytes);
            assertEquals(testValue, (short) recovered);
        }

        @Test
        void testByteToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(ByteSerializer.INSTANCE);
            assertNotNull(info);
            
            byte testValue = (byte) 0xAB;
            byte[] bytes = info.toBytes(testValue);
            assertEquals(1, bytes.length);
            
            Byte recovered = info.fromBytes(bytes);
            assertEquals(testValue, (byte) recovered);
        }

        @Test
        void testBooleanToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(BooleanSerializer.INSTANCE);
            assertNotNull(info);
            
            byte[] bytesTrue = info.toBytes(true);
            byte[] bytesFalse = info.toBytes(false);
            
            assertEquals(1, bytesTrue.length);
            assertEquals(1, bytesFalse.length);
            
            assertTrue(info.<Boolean>fromBytes(bytesTrue));
            assertFalse(info.<Boolean>fromBytes(bytesFalse));
        }

        @Test
        void testCharToFromBytes() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(CharSerializer.INSTANCE);
            assertNotNull(info);
            
            char testValue = '中';  // Unicode character
            byte[] bytes = info.toBytes(testValue);
            assertEquals(2, bytes.length);
            
            Character recovered = info.fromBytes(bytes);
            assertEquals(testValue, (char) recovered);
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testLongMinMax() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(LongSerializer.INSTANCE);
            assertNotNull(info);
            
            assertEquals(Long.MIN_VALUE, (long) info.fromBytes(info.toBytes(Long.MIN_VALUE)));
            assertEquals(Long.MAX_VALUE, (long) info.fromBytes(info.toBytes(Long.MAX_VALUE)));
            assertEquals(0L, (long) info.fromBytes(info.toBytes(0L)));
        }

        @Test
        void testDoubleSpecialValues() {
            FixedLengthTypeSupport.TypeInfo info = FixedLengthTypeSupport.detect(DoubleSerializer.INSTANCE);
            assertNotNull(info);
            
            assertEquals(Double.POSITIVE_INFINITY, info.fromBytes(info.toBytes(Double.POSITIVE_INFINITY)));
            assertEquals(Double.NEGATIVE_INFINITY, info.fromBytes(info.toBytes(Double.NEGATIVE_INFINITY)));
            assertTrue(Double.isNaN(info.<Double>fromBytes(info.toBytes(Double.NaN))));
        }
    }
}
