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
         * Uses native byte order for optimal performance.
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
         * Uses native byte order for optimal performance.
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
         * Reads Tuple directly from MemorySegment (zero-copy fast path).
         * Uses native byte order for optimal performance.
         * @param segment The memory segment to read from
         * @param baseOffset The offset where the tuple data starts
         */
        public <S extends Tuple> S read(MemorySegment segment, int baseOffset) {
            Object[] fields = new Object[arity];
            
            for (int i = 0; i < arity; i++) {
                int offset = baseOffset + fieldOffsets[i];
                FixedType ft = fieldTypes[i];
                
                switch (ft) {
                    case LONG:
                        fields[i] = segment.getLong(offset);
                        break;
                    case INT:
                        fields[i] = segment.getInt(offset);
                        break;
                    case DOUBLE:
                        fields[i] = segment.getDouble(offset);
                        break;
                    case FLOAT:
                        fields[i] = segment.getFloat(offset);
                        break;
                    case SHORT:
                        fields[i] = segment.getShort(offset);
                        break;
                    case BYTE:
                        fields[i] = segment.get(offset);
                        break;
                    case BOOLEAN:
                        fields[i] = segment.get(offset) != 0;
                        break;
                    case CHAR:
                        fields[i] = segment.getChar(offset);
                        break;
                }
            }
            
            return (S) createTuple(fields);
        }
        
        /**
         * Writes Tuple directly to MemorySegment (zero-copy fast path).
         * Uses native byte order for optimal performance.
         * @param segment The memory segment to write to
         * @param baseOffset The offset where the tuple data starts
         * @param tuple The tuple to write
         */
        public <S extends Tuple> void write(MemorySegment segment, int baseOffset, S tuple) {
            for (int i = 0; i < arity; i++) {
                Object fieldValue = tuple.getField(i);
                int offset = baseOffset + fieldOffsets[i];
                FixedType ft = fieldTypes[i];
                
                switch (ft) {
                    case LONG:
                        segment.putLong(offset, (Long) fieldValue);
                        break;
                    case INT:
                        segment.putInt(offset, (Integer) fieldValue);
                        break;
                    case DOUBLE:
                        segment.putDouble(offset, (Double) fieldValue);
                        break;
                    case FLOAT:
                        segment.putFloat(offset, (Float) fieldValue);
                        break;
                    case SHORT:
                        segment.putShort(offset, (Short) fieldValue);
                        break;
                    case BYTE:
                        segment.put(offset, (Byte) fieldValue);
                        break;
                    case BOOLEAN:
                        segment.put(offset, (byte) ((Boolean) fieldValue ? 1 : 0));
                        break;
                    case CHAR:
                        segment.putChar(offset, (Character) fieldValue);
                        break;
                }
            }
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
