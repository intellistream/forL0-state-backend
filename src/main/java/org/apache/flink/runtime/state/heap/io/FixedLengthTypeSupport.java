package org.apache.flink.runtime.state.heap.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.memory.MemorySegment;

import java.lang.reflect.Method;

/**
 * Fast-path support for fixed-length types.
 * Provides direct memory read/write operations to bypass serialization overhead.
 * 
 * <p>Supported types:
 * <ul>
 *   <li>Long (8 bytes)</li>
 *   <li>Integer (4 bytes)</li>
 *   <li>Double (8 bytes)</li>
 *   <li>Float (4 bytes)</li>
 *   <li>Short (2 bytes)</li>
 *   <li>Byte (1 byte)</li>
 *   <li>Boolean (1 byte)</li>
 *   <li>Character (2 bytes)</li>
 * </ul>
 * 
 * <p>Usage pattern:
 * <pre>{@code
 * TypeSerializer<S> stateSerializer = ...;
 * FixedLengthTypeSupport.TypeInfo typeInfo = FixedLengthTypeSupport.detect(stateSerializer);
 * if (typeInfo != null) {
 *     // Fast path: direct memory access
 *     long value = typeInfo.readLong(segment, offset);
 *     typeInfo.writeLong(segment, offset, newValue);
 * } else {
 *     // Normal path: use serializer
 * }
 * }</pre>
 */
public final class FixedLengthTypeSupport {
    
    private FixedLengthTypeSupport() {}
    
    /**
     * Fixed-length type enum with size and memory operations.
     */
    public enum FixedType {
        LONG(8, "LongSerializer"),
        INT(4, "IntSerializer"),
        DOUBLE(8, "DoubleSerializer"),
        FLOAT(4, "FloatSerializer"),
        SHORT(2, "ShortSerializer"),
        BYTE(1, "ByteSerializer"),
        BOOLEAN(1, "BooleanSerializer"),
        CHAR(2, "CharSerializer");
        
        private final int byteSize;
        private final String serializerSimpleName;
        
        FixedType(int byteSize, String serializerSimpleName) {
            this.byteSize = byteSize;
            this.serializerSimpleName = serializerSimpleName;
        }
        
        public int getByteSize() {
            return byteSize;
        }
        
        public String getSerializerSimpleName() {
            return serializerSimpleName;
        }
    }
    
    /**
     * Type information holder for fast-path operations.
     */
    public static final class TypeInfo {
        private final FixedType type;
        
        TypeInfo(FixedType type) {
            this.type = type;
        }
        
        public FixedType getType() {
            return type;
        }
        
        public int getByteSize() {
            return type.getByteSize();
        }
        
        /**
         * Reads value directly from memory segment as Object.
         */
        @SuppressWarnings("unchecked")
        public <S> S read(MemorySegment segment, int offset) {
            switch (type) {
                case LONG:
                    return (S) Long.valueOf(segment.getLong(offset));
                case INT:
                    return (S) Integer.valueOf(segment.getInt(offset));
                case DOUBLE:
                    return (S) Double.valueOf(segment.getDouble(offset));
                case FLOAT:
                    return (S) Float.valueOf(segment.getFloat(offset));
                case SHORT:
                    return (S) Short.valueOf(segment.getShort(offset));
                case BYTE:
                    return (S) Byte.valueOf(segment.get(offset));
                case BOOLEAN:
                    return (S) Boolean.valueOf(segment.get(offset) != 0);
                case CHAR:
                    return (S) Character.valueOf(segment.getChar(offset));
                default:
                    throw new IllegalStateException("Unknown type: " + type);
            }
        }
        
        /**
         * Writes value directly to memory segment.
         */
        public <S> void write(MemorySegment segment, int offset, S value) {
            switch (type) {
                case LONG:
                    segment.putLong(offset, (Long) value);
                    break;
                case INT:
                    segment.putInt(offset, (Integer) value);
                    break;
                case DOUBLE:
                    segment.putDouble(offset, (Double) value);
                    break;
                case FLOAT:
                    segment.putFloat(offset, (Float) value);
                    break;
                case SHORT:
                    segment.putShort(offset, (Short) value);
                    break;
                case BYTE:
                    segment.put(offset, (Byte) value);
                    break;
                case BOOLEAN:
                    segment.put(offset, (byte) ((Boolean) value ? 1 : 0));
                    break;
                case CHAR:
                    segment.putChar(offset, (Character) value);
                    break;
                default:
                    throw new IllegalStateException("Unknown type: " + type);
            }
        }
        
        /**
         * Writes value to byte array (for EntryArena).
         */
        public <S> byte[] toBytes(S value) {
            byte[] bytes = new byte[type.getByteSize()];
            toBytesInto(value, bytes);
            return bytes;
        }
        
        /**
         * Writes value into provided buffer (avoids allocation).
         * Buffer must be at least getByteSize() bytes.
         */
        public <S> void toBytesInto(S value, byte[] buffer) {
            switch (type) {
                case LONG:
                    long longVal = (Long) value;
                    buffer[0] = (byte) (longVal >> 56);
                    buffer[1] = (byte) (longVal >> 48);
                    buffer[2] = (byte) (longVal >> 40);
                    buffer[3] = (byte) (longVal >> 32);
                    buffer[4] = (byte) (longVal >> 24);
                    buffer[5] = (byte) (longVal >> 16);
                    buffer[6] = (byte) (longVal >> 8);
                    buffer[7] = (byte) longVal;
                    break;
                case INT:
                    int intVal = (Integer) value;
                    buffer[0] = (byte) (intVal >> 24);
                    buffer[1] = (byte) (intVal >> 16);
                    buffer[2] = (byte) (intVal >> 8);
                    buffer[3] = (byte) intVal;
                    break;
                case DOUBLE:
                    long doubleBits = Double.doubleToRawLongBits((Double) value);
                    buffer[0] = (byte) (doubleBits >> 56);
                    buffer[1] = (byte) (doubleBits >> 48);
                    buffer[2] = (byte) (doubleBits >> 40);
                    buffer[3] = (byte) (doubleBits >> 32);
                    buffer[4] = (byte) (doubleBits >> 24);
                    buffer[5] = (byte) (doubleBits >> 16);
                    buffer[6] = (byte) (doubleBits >> 8);
                    buffer[7] = (byte) doubleBits;
                    break;
                case FLOAT:
                    int floatBits = Float.floatToRawIntBits((Float) value);
                    buffer[0] = (byte) (floatBits >> 24);
                    buffer[1] = (byte) (floatBits >> 16);
                    buffer[2] = (byte) (floatBits >> 8);
                    buffer[3] = (byte) floatBits;
                    break;
                case SHORT:
                    short shortVal = (Short) value;
                    buffer[0] = (byte) (shortVal >> 8);
                    buffer[1] = (byte) shortVal;
                    break;
                case BYTE:
                    buffer[0] = (Byte) value;
                    break;
                case BOOLEAN:
                    buffer[0] = (byte) ((Boolean) value ? 1 : 0);
                    break;
                case CHAR:
                    char charVal = (Character) value;
                    buffer[0] = (byte) (charVal >> 8);
                    buffer[1] = (byte) charVal;
                    break;
                default:
                    throw new IllegalStateException("Unknown type: " + type);
            }
        }
        
        /**
         * Reads value from byte array.
         */
        @SuppressWarnings("unchecked")
        public <S> S fromBytes(byte[] bytes) {
            switch (type) {
                case LONG:
                    long longVal = 0;
                    for (int i = 0; i < 8; i++) {
                        longVal = (longVal << 8) | (bytes[i] & 0xFF);
                    }
                    return (S) Long.valueOf(longVal);
                case INT:
                    int intVal = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) |
                                 ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
                    return (S) Integer.valueOf(intVal);
                case DOUBLE:
                    long doubleBits = 0;
                    for (int i = 0; i < 8; i++) {
                        doubleBits = (doubleBits << 8) | (bytes[i] & 0xFF);
                    }
                    return (S) Double.valueOf(Double.longBitsToDouble(doubleBits));
                case FLOAT:
                    int floatBits = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) |
                                    ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
                    return (S) Float.valueOf(Float.intBitsToFloat(floatBits));
                case SHORT:
                    short shortVal = (short) (((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
                    return (S) Short.valueOf(shortVal);
                case BYTE:
                    return (S) Byte.valueOf(bytes[0]);
                case BOOLEAN:
                    return (S) Boolean.valueOf(bytes[0] != 0);
                case CHAR:
                    char charVal = (char) (((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
                    return (S) Character.valueOf(charVal);
                default:
                    throw new IllegalStateException("Unknown type: " + type);
            }
        }
    }
    
    /**
     * Type information holder for Tuple types composed of fixed-length fields.
     * Supports Tuple2 through Tuple8 where all fields are fixed-length primitive types.
     */
    public static final class TupleTypeInfo {
        private final int arity;
        private final FixedType[] fieldTypes;
        private final int[] fieldOffsets;
        private final int totalSize;
        
        TupleTypeInfo(int arity, FixedType[] fieldTypes) {
            this.arity = arity;
            this.fieldTypes = fieldTypes;
            this.fieldOffsets = new int[arity];
            
            int offset = 0;
            for (int i = 0; i < arity; i++) {
                fieldOffsets[i] = offset;
                offset += fieldTypes[i].getByteSize();
            }
            this.totalSize = offset;
        }
        
        public int getArity() {
            return arity;
        }
        
        public int getByteSize() {
            return totalSize;
        }
        
        public FixedType[] getFieldTypes() {
            return fieldTypes;
        }
        
        /**
         * Writes Tuple to byte array.
         */
        @SuppressWarnings("unchecked")
        public <S extends Tuple> byte[] toBytes(S tuple) {
            byte[] bytes = new byte[totalSize];
            toBytesInto(tuple, bytes);
            return bytes;
        }
        
        /**
         * Writes Tuple into provided buffer (avoids allocation).
         * Buffer must be at least getByteSize() bytes.
         */
        @SuppressWarnings("unchecked")
        public <S extends Tuple> void toBytesInto(S tuple, byte[] buffer) {
            for (int i = 0; i < arity; i++) {
                Object fieldValue = tuple.getField(i);
                int offset = fieldOffsets[i];
                FixedType ft = fieldTypes[i];
                
                switch (ft) {
                    case LONG:
                        writeLongToBytes(buffer, offset, (Long) fieldValue);
                        break;
                    case INT:
                        writeIntToBytes(buffer, offset, (Integer) fieldValue);
                        break;
                    case DOUBLE:
                        writeLongToBytes(buffer, offset, Double.doubleToRawLongBits((Double) fieldValue));
                        break;
                    case FLOAT:
                        writeIntToBytes(buffer, offset, Float.floatToRawIntBits((Float) fieldValue));
                        break;
                    case SHORT:
                        writeShortToBytes(buffer, offset, (Short) fieldValue);
                        break;
                    case BYTE:
                        buffer[offset] = (Byte) fieldValue;
                        break;
                    case BOOLEAN:
                        buffer[offset] = (byte) ((Boolean) fieldValue ? 1 : 0);
                        break;
                    case CHAR:
                        writeShortToBytes(buffer, offset, (short) ((Character) fieldValue).charValue());
                        break;
                }
            }
        }
        
        /**
         * Reads Tuple from byte array.
         */
        @SuppressWarnings("unchecked")
        public <S extends Tuple> S fromBytes(byte[] bytes) {
            Object[] fields = new Object[arity];
            
            for (int i = 0; i < arity; i++) {
                int offset = fieldOffsets[i];
                FixedType ft = fieldTypes[i];
                
                switch (ft) {
                    case LONG:
                        fields[i] = readLongFromBytes(bytes, offset);
                        break;
                    case INT:
                        fields[i] = readIntFromBytes(bytes, offset);
                        break;
                    case DOUBLE:
                        fields[i] = Double.longBitsToDouble(readLongFromBytes(bytes, offset));
                        break;
                    case FLOAT:
                        fields[i] = Float.intBitsToFloat(readIntFromBytes(bytes, offset));
                        break;
                    case SHORT:
                        fields[i] = readShortFromBytes(bytes, offset);
                        break;
                    case BYTE:
                        fields[i] = bytes[offset];
                        break;
                    case BOOLEAN:
                        fields[i] = bytes[offset] != 0;
                        break;
                    case CHAR:
                        fields[i] = (char) readShortFromBytes(bytes, offset);
                        break;
                }
            }
            
            return (S) createTuple(fields);
        }
        
        /**
         * Creates Tuple instance by arity (avoids reflection overhead).
         */
        private Tuple createTuple(Object[] fields) {
            switch (arity) {
                case 2:
                    return Tuple2.of(fields[0], fields[1]);
                case 3:
                    return Tuple3.of(fields[0], fields[1], fields[2]);
                case 4:
                    return Tuple4.of(fields[0], fields[1], fields[2], fields[3]);
                case 5:
                    return Tuple5.of(fields[0], fields[1], fields[2], fields[3], fields[4]);
                case 6:
                    return Tuple6.of(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
                case 7:
                    return Tuple7.of(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]);
                case 8:
                    return Tuple8.of(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]);
                default:
                    throw new IllegalStateException("Unsupported tuple arity: " + arity);
            }
        }
        
        // Helper methods for byte array operations
        private static void writeLongToBytes(byte[] bytes, int offset, long value) {
            bytes[offset]     = (byte) (value >> 56);
            bytes[offset + 1] = (byte) (value >> 48);
            bytes[offset + 2] = (byte) (value >> 40);
            bytes[offset + 3] = (byte) (value >> 32);
            bytes[offset + 4] = (byte) (value >> 24);
            bytes[offset + 5] = (byte) (value >> 16);
            bytes[offset + 6] = (byte) (value >> 8);
            bytes[offset + 7] = (byte) value;
        }
        
        private static void writeIntToBytes(byte[] bytes, int offset, int value) {
            bytes[offset]     = (byte) (value >> 24);
            bytes[offset + 1] = (byte) (value >> 16);
            bytes[offset + 2] = (byte) (value >> 8);
            bytes[offset + 3] = (byte) value;
        }
        
        private static void writeShortToBytes(byte[] bytes, int offset, short value) {
            bytes[offset]     = (byte) (value >> 8);
            bytes[offset + 1] = (byte) value;
        }
        
        private static long readLongFromBytes(byte[] bytes, int offset) {
            return ((long)(bytes[offset] & 0xFF) << 56) |
                   ((long)(bytes[offset + 1] & 0xFF) << 48) |
                   ((long)(bytes[offset + 2] & 0xFF) << 40) |
                   ((long)(bytes[offset + 3] & 0xFF) << 32) |
                   ((long)(bytes[offset + 4] & 0xFF) << 24) |
                   ((long)(bytes[offset + 5] & 0xFF) << 16) |
                   ((long)(bytes[offset + 6] & 0xFF) << 8) |
                   (bytes[offset + 7] & 0xFF);
        }
        
        private static int readIntFromBytes(byte[] bytes, int offset) {
            return ((bytes[offset] & 0xFF) << 24) |
                   ((bytes[offset + 1] & 0xFF) << 16) |
                   ((bytes[offset + 2] & 0xFF) << 8) |
                   (bytes[offset + 3] & 0xFF);
        }
        
        private static short readShortFromBytes(byte[] bytes, int offset) {
            return (short) (((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF));
        }
    }
    
    /**
     * Detects if the given serializer is a fixed-length primitive type.
     * 
     * @param serializer The type serializer to check
     * @return TypeInfo if it's a supported fixed-length type, null otherwise
     */
    public static TypeInfo detect(TypeSerializer<?> serializer) {
        if (serializer == null) {
            return null;
        }
        
        String className = serializer.getClass().getSimpleName();
        
        for (FixedType type : FixedType.values()) {
            if (type.getSerializerSimpleName().equals(className)) {
                return new TypeInfo(type);
            }
        }
        
        return null;
    }
    
    /**
     * Detects if the given serializer is a Tuple with all fixed-length fields.
     * Uses reflection to extract field serializers from TupleSerializer.
     * 
     * @param serializer The type serializer to check
     * @return TupleTypeInfo if it's a supported fixed-length Tuple, null otherwise
     */
    public static TupleTypeInfo detectTuple(TypeSerializer<?> serializer) {
        if (serializer == null) {
            return null;
        }
        
        String className = serializer.getClass().getSimpleName();
        if (!"TupleSerializer".equals(className)) {
            return null;
        }
        
        try {
            // Get fieldSerializers via reflection
            java.lang.reflect.Method getFieldSerializers = 
                serializer.getClass().getMethod("getFieldSerializers");
            TypeSerializer<?>[] fieldSerializers = 
                (TypeSerializer<?>[]) getFieldSerializers.invoke(serializer);
            
            int arity = fieldSerializers.length;
            if (arity < 2 || arity > 8) {
                // Only support Tuple2 to Tuple8 for now
                return null;
            }
            
            FixedType[] fieldTypes = new FixedType[arity];
            
            // Check each field is a fixed-length type
            for (int i = 0; i < arity; i++) {
                TypeInfo fieldTypeInfo = detect(fieldSerializers[i]);
                if (fieldTypeInfo == null) {
                    // This field is not a fixed-length type
                    return null;
                }
                fieldTypes[i] = fieldTypeInfo.getType();
            }
            
            return new TupleTypeInfo(arity, fieldTypes);
            
        } catch (Exception e) {
            // Reflection failed, fall back to normal path
            return null;
        }
    }
    
    /**
     * Checks if the serializer is a fixed-length type.
     */
    public static boolean isFixedLengthType(TypeSerializer<?> serializer) {
        return detect(serializer) != null;
    }
}
