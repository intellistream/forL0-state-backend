package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

/**
 * Analyzes Flink TypeSerializer instances and generates TypeLayout descriptor byte streams
 * for the C++ engine. This is the "compiler" that maps Java dynamic types to C++ static types.
 *
 * <p>Runs on the cold path (state registration), not on the hot path.
 *
 * <p>TypeId encoding:
 * <ul>
 *   <li>1 = INT32</li>
 *   <li>2 = INT64</li>
 *   <li>3 = FLOAT32</li>
 *   <li>4 = FLOAT64</li>
 *   <li>5 = BOOL</li>
 *   <li>6 = STRING</li>
 *   <li>7 = BYTES (byte[])</li>
 *   <li>10 = STRUCT</li>
 *   <li>11 = LIST</li>
 *   <li>12 = MAP</li>
 *   <li>13 = FIXED_ROW (FixedLengthRow — multi-field all-fixed RowData)</li>
 *   <li>20 = VOID_NS (VoidNamespace)</li>
 * </ul>
 */
public final class TypeAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(TypeAnalyzer.class);

    // Type ID constants — must match C++ TypeId enum in type_layout.h
    public static final int TYPE_INT32    = 1;
    public static final int TYPE_INT64    = 2;
    public static final int TYPE_FLOAT32  = 3;
    public static final int TYPE_FLOAT64  = 4;
    public static final int TYPE_BOOL     = 5;
    public static final int TYPE_STRING   = 6;
    public static final int TYPE_BYTES    = 7;
    public static final int TYPE_STRUCT   = 10;
    public static final int TYPE_LIST     = 11;
    public static final int TYPE_MAP      = 12;
    public static final int TYPE_FIXED_ROW = 13;
    private static final int MAX_FIXED_ROW_ARITY = 8;
    public static final int TYPE_VOID_NS  = 20;
    public static final int TYPE_TIME_WINDOW = 21;

    /** Cached RowDataSerializer class reference (loaded via reflection to avoid hard dep). */
    private static final Class<?> ROW_DATA_SERIALIZER_CLASS;
    /** Cached 'types' field in RowDataSerializer (LogicalType[]). */
    private static final Field ROW_DATA_TYPES_FIELD;
    private static final MethodHandle ROW_DATA_TO_BINARY;
    /** Cached TimeWindowSerializer class reference (loaded via reflection). */
    private static final Class<?> TIME_WINDOW_SERIALIZER_CLASS;
    private static final Class<?> SQL_TIME_WINDOW_SERIALIZER_CLASS;
    private static final MethodHandle SQL_WINDOW_START, SQL_WINDOW_END, SQL_WINDOW_CONSTRUCTOR;

    static {
        Class<?> cls = null;
        Field f = null;
        MethodHandle converter = null;
        try {
            cls = Class.forName("org.apache.flink.table.runtime.typeutils.RowDataSerializer");
            f = cls.getDeclaredField("types");
            f.setAccessible(true);
            converter = MethodHandles.publicLookup().findVirtual(cls, "toBinaryRow",
                    MethodType.methodType(BinaryRowData.class, RowData.class))
                    .asType(MethodType.methodType(BinaryRowData.class, TypeSerializer.class, RowData.class));
        } catch (ReflectiveOperationException e) {
            // flink-table-runtime not on classpath — RowData optimization unavailable
        }
        ROW_DATA_SERIALIZER_CLASS = cls;
        ROW_DATA_TYPES_FIELD = f;
        ROW_DATA_TO_BINARY = converter;

        Class<?> twCls = null;
        try {
            twCls = Class.forName("org.apache.flink.streaming.api.windowing.windows.TimeWindow$Serializer");
        } catch (ClassNotFoundException e) {
            // flink-streaming not on classpath
        }
        TIME_WINDOW_SERIALIZER_CLASS = twCls;
        Class<?> sqlSerializer = null;
        MethodHandle start = null, end = null, constructor = null;
        try {
            Class<?> window = Class.forName("org.apache.flink.table.runtime.operators.window.TimeWindow");
            sqlSerializer = Class.forName(window.getName() + "$Serializer");
            start = MethodHandles.publicLookup().findVirtual(window, "getStart", MethodType.methodType(long.class))
                    .asType(MethodType.methodType(long.class, Object.class));
            end = MethodHandles.publicLookup().findVirtual(window, "getEnd", MethodType.methodType(long.class))
                    .asType(MethodType.methodType(long.class, Object.class));
            constructor = MethodHandles.publicLookup().findConstructor(window, MethodType.methodType(void.class, long.class, long.class))
                    .asType(MethodType.methodType(Object.class, long.class, long.class));
        } catch (ReflectiveOperationException ignored) {
            // SQL window support remains optional for DataStream deployments.
        }
        SQL_TIME_WINDOW_SERIALIZER_CLASS = sqlSerializer;
        SQL_WINDOW_START = start;
        SQL_WINDOW_END = end;
        SQL_WINDOW_CONSTRUCTOR = constructor;
    }

    private TypeAnalyzer() {}

    /** Cache the optional table-runtime conversion handle on the registration path. */
    static BinaryRowData toBinaryRow(TypeSerializer<?> serializer, RowData value) {
        if (value instanceof BinaryRowData) return (BinaryRowData) value;
        try {
            return (BinaryRowData) ROW_DATA_TO_BINARY.invokeExact(serializer, value);
        } catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("Cannot convert RowData to binary", e); }
    }

    /**
     * Determine the TypeId for a given TypeSerializer.
     * Returns -1 if the type is unsupported.
     */
    public static int getTypeId(TypeSerializer<?> serializer) {
        if (serializer instanceof IntSerializer || serializer instanceof ShortSerializer
                || serializer instanceof ByteSerializer) {
            return TYPE_INT32;
        } else if (serializer instanceof LongSerializer) {
            return TYPE_INT64;
        } else if (serializer instanceof FloatSerializer) {
            return TYPE_FLOAT32;
        } else if (serializer instanceof DoubleSerializer) {
            return TYPE_FLOAT64;
        } else if (serializer instanceof BooleanSerializer) {
            return TYPE_BOOL;
        } else if (serializer instanceof StringSerializer) {
            return TYPE_STRING;
        } else if (serializer instanceof BytePrimitiveArraySerializer) {
            return TYPE_BYTES;
        } else if (serializer instanceof VoidNamespaceSerializer) {
            return TYPE_VOID_NS;
        } else if (isTimeWindowSerializer(serializer)) {
            return TYPE_TIME_WINDOW;
        } else if (serializer instanceof ListSerializer) {
            return TYPE_LIST;
        } else if (serializer instanceof MapSerializer) {
            return TYPE_MAP;
        }

        // RowDataSerializer detection via reflection
        LogicalType[] rowTypes = getRowDataFieldTypes(serializer);
        if (rowTypes != null) {
            return getRowDataTypeId(rowTypes);
        }

        LOG.warn("[ForL0] Unsupported TypeSerializer: {}. Using generic serialized path.",
                serializer.getClass().getName());
        return -1;
    }

    /**
     * Determine the effective TypeId for a RowData key.
     * <ul>
     *   <li>Single BIGINT/TIMESTAMP → TYPE_INT64 (unwrap)</li>
     *   <li>Single INT/DATE → TYPE_INT32 (unwrap)</li>
     *   <li>All fixed-length fields → TYPE_FIXED_ROW</li>
     *   <li>Otherwise → -1 (generic)</li>
     * </ul>
     */
    static int getRowDataTypeId(LogicalType[] fieldTypes) {
        if (fieldTypes.length == 1) {
            // Single-field unwrap: return the primitive type
            return singleFieldTypeId(fieldTypes[0]);
        }

        // Multi-field: check if all fields are fixed-length
        for (LogicalType t : fieldTypes) {
            if (!RowDataKeyAccessor.isFixedLengthAsLong(t)) {
                LOG.info("[ForL0] RowData has variable-length field ({}), using generic path.",
                        t.getTypeRoot());
                return -1;
            }
        }

        if (fieldTypes.length <= MAX_FIXED_ROW_ARITY) {
            return TYPE_FIXED_ROW;
        }
        LOG.info("[ForL0] RowData fixed-length arity {} exceeds native FixedRow limit {}, using generic path.",
                fieldTypes.length, MAX_FIXED_ROW_ARITY);
        return -1;
    }

    /**
     * Map a single RowData field LogicalType to a primitive TypeId for unwrapping.
     */
    private static int singleFieldTypeId(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_DAY_TIME:
                return TYPE_INT64;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case SMALLINT:
            case TINYINT:
                return TYPE_INT32;
            case FLOAT:
                return TYPE_FLOAT32;
            case DOUBLE:
                return TYPE_FLOAT64;
            case BOOLEAN:
                return TYPE_BOOL;
            default:
                return -1;
        }
    }

    /**
     * Extract LogicalType[] from a RowDataSerializer via reflection.
     * Returns null if serializer is not RowDataSerializer or extraction fails.
     */
    static LogicalType[] getRowDataFieldTypes(TypeSerializer<?> serializer) {
        if (ROW_DATA_SERIALIZER_CLASS == null || ROW_DATA_TYPES_FIELD == null) {
            return null;
        }
        if (!ROW_DATA_SERIALIZER_CLASS.isInstance(serializer)) {
            return null;
        }
        try {
            return (LogicalType[]) ROW_DATA_TYPES_FIELD.get(serializer);
        } catch (IllegalAccessException e) {
            LOG.warn("[ForL0] Failed to extract LogicalType[] from RowDataSerializer", e);
            return null;
        }
    }

    /**
     * Check if a TypeSerializer is a RowDataSerializer.
     */
    public static boolean isRowDataSerializer(TypeSerializer<?> serializer) {
        return ROW_DATA_SERIALIZER_CLASS != null
                && ROW_DATA_SERIALIZER_CLASS.isInstance(serializer);
    }

    /**
     * Create a RowDataKeyAccessor for a RowDataSerializer.
     * Returns null if the serializer is not RowDataSerializer.
     */
    public static RowDataKeyAccessor createRowDataKeyAccessor(TypeSerializer<?> serializer) {
        LogicalType[] types = getRowDataFieldTypes(serializer);
        if (types == null) {
            return null;
        }
        return RowDataKeyAccessor.create(types);
    }

    /**
     * Generate a TypeLayout descriptor byte stream for the given TypeSerializer.
     * This descriptor is sent to C++ via JNI during state registration.
     */
    public static byte[] generateDescriptor(TypeSerializer<?> serializer) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
            DataOutputStream out = new DataOutputStream(baos);
            writeDescriptor(out, serializer);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("[ForL0] Failed to generate type descriptor", e);
        }
    }

    private static void writeDescriptor(DataOutputStream out, TypeSerializer<?> serializer)
            throws IOException {
        int typeId = getTypeId(serializer);
        // SQL windows keep generic namespace dispatch for checkpoint compatibility,
        // but nested map/list readers must know their fixed 16-byte encoding.
        if (isSqlTimeWindowSerializer(serializer)) {
            out.writeByte(TYPE_TIME_WINDOW);
            return;
        }

        if (typeId == TYPE_LIST) {
            out.writeByte(TYPE_LIST);
            @SuppressWarnings("rawtypes")
            ListSerializer listSer = (ListSerializer) serializer;
            writeDescriptor(out, listSer.getElementSerializer());
        } else if (typeId == TYPE_MAP) {
            out.writeByte(TYPE_MAP);
            @SuppressWarnings("rawtypes")
            MapSerializer mapSer = (MapSerializer) serializer;
            writeDescriptor(out, mapSer.getKeySerializer());
            writeDescriptor(out, mapSer.getValueSerializer());
        } else if (typeId == TYPE_FIXED_ROW) {
            // FixedLengthRow: [TYPE_FIXED_ROW][arity(2 bytes)]
            LogicalType[] types = getRowDataFieldTypes(serializer);
            out.writeByte(TYPE_FIXED_ROW);
            out.writeShort(types != null ? types.length : 0);
        } else if (typeId >= 0) {
            // Primitive, string, bytes, or void namespace — single byte
            out.writeByte(typeId);
        } else {
            // Unsupported — encode as BYTES (will use generic serialized path)
            out.writeByte(TYPE_BYTES);
        }
    }

    /**
     * Check if a namespace serializer represents VoidNamespace.
     */
    public static boolean isVoidNamespace(TypeSerializer<?> namespaceSerializer) {
        return namespaceSerializer instanceof VoidNamespaceSerializer;
    }

    /**
     * Check if a TypeSerializer is a TimeWindow.Serializer.
     */
    public static boolean isTimeWindowSerializer(TypeSerializer<?> serializer) {
        return TIME_WINDOW_SERIALIZER_CLASS != null
                && TIME_WINDOW_SERIALIZER_CLASS.isInstance(serializer);
    }

    static boolean isSqlTimeWindowSerializer(TypeSerializer<?> serializer) {
        return SQL_TIME_WINDOW_SERIALIZER_CLASS != null && SQL_TIME_WINDOW_SERIALIZER_CLASS.isInstance(serializer);
    }

    static boolean isAnyTimeWindowSerializer(TypeSerializer<?> serializer) {
        return isTimeWindowSerializer(serializer) || isSqlTimeWindowSerializer(serializer);
    }

    static long windowStart(Object window) {
        if (window instanceof org.apache.flink.streaming.api.windowing.windows.TimeWindow)
            return ((org.apache.flink.streaming.api.windowing.windows.TimeWindow) window).getStart();
        try { return (long) SQL_WINDOW_START.invokeExact(window); }
        catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("Cannot access SQL window", e); }
    }

    static long windowEnd(Object window) {
        if (window instanceof org.apache.flink.streaming.api.windowing.windows.TimeWindow)
            return ((org.apache.flink.streaming.api.windowing.windows.TimeWindow) window).getEnd();
        try { return (long) SQL_WINDOW_END.invokeExact(window); }
        catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("Cannot access SQL window", e); }
    }

    static Object timeWindow(TypeSerializer<?> serializer, long start, long end) {
        if (isTimeWindowSerializer(serializer))
            return new org.apache.flink.streaming.api.windowing.windows.TimeWindow(start, end);
        try { return (Object) SQL_WINDOW_CONSTRUCTOR.invokeExact(start, end); }
        catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("Cannot construct SQL window", e); }
    }

    /**
     * Generate combined descriptor for a state (key + namespace + value).
     */
    public static byte[] generateStateDescriptor(
            TypeSerializer<?> keySerializer,
            TypeSerializer<?> namespaceSerializer,
            TypeSerializer<?> valueSerializer) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
            DataOutputStream out = new DataOutputStream(baos);
            writeDescriptor(out, keySerializer);
            writeDescriptor(out, namespaceSerializer);
            writeDescriptor(out, valueSerializer);
            // Optional physical-row hints; old native parsers ignore the tail.
            out.writeByte(0xC7);
            out.writeByte((isRowDataSerializer(keySerializer) ? 1 : 0)
                    | (isRowDataSerializer(valueSerializer) ? 2 : 0));
            // Logical types select native tables; these optional hints describe
            // bytes produced by Java serializers inside native containers.
            out.writeByte(0xC8);
            writeSerializedLayout(out, valueSerializer);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("[ForL0] Failed to generate state type descriptor", e);
        }
    }

    private static void writeSerializedLayout(DataOutputStream out, TypeSerializer<?> serializer)
            throws IOException {
        boolean list = serializer instanceof ListSerializer;
        boolean map = serializer instanceof MapSerializer;
        out.writeByte((isRowDataSerializer(serializer) ? 1 : 0) | (list || map ? 2 : 0));
        out.writeInt(serializer == null ? -1 : serializer.getLength());
        if (list) {
            writeSerializedLayout(out, ((ListSerializer<?>) serializer).getElementSerializer());
        } else if (map) {
            writeSerializedLayout(out, ((MapSerializer<?, ?>) serializer).getKeySerializer());
            writeSerializedLayout(out, ((MapSerializer<?, ?>) serializer).getValueSerializer());
        }
    }
}
