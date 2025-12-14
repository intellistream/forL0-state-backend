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
}
