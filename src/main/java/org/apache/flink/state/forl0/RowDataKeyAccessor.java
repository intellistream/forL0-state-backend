package org.apache.flink.state.forl0;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Extracts key fields from {@link RowData} objects for the JNI fast path.
 *
 * <p>Created at state registration time (cold path). On the hot path,
 * uses type-specific {@link RowData} getters ({@code getLong}, {@code getInt}, etc.)
 * to extract fields without any serialization.
 *
 * <p>Three strategies based on field composition:
 * <ul>
 *   <li><b>Single-field primitive</b>: Unwrap to primitive value (e.g., RowData[BIGINT] → long).
 *       Uses the same fast path as DataStream primitive keys.</li>
 *   <li><b>Multi-field all-fixed</b>: Extract to {@code long[]} (FixedLengthRow).
 *       Each fixed-length field is stored as an int64_t.</li>
 *   <li><b>Variable-length</b>: Falls back to generic serialized path.</li>
 * </ul>
 */
public final class RowDataKeyAccessor {

    /** Strategy types determined at construction time. */
    public enum Strategy {
        /** Single field primitive — unwrap to long/int/double. */
        SINGLE_LONG,
        SINGLE_INT,
        SINGLE_DOUBLE,
        SINGLE_FLOAT,
        SINGLE_BOOLEAN,
        /** Multi-field, all fixed-length — extract to long[]. */
        FIXED_LENGTH_ROW,
        /** Contains variable-length fields — use generic path. */
        GENERIC
    }

    private final Strategy strategy;
    private final int arity;
    private final LogicalType[] fieldTypes;
    /** Reusable buffer for multi-field extraction (single-threaded). */
    private final long[] fieldBuffer;

    private RowDataKeyAccessor(Strategy strategy, int arity, LogicalType[] fieldTypes) {
        this.strategy = strategy;
        this.arity = arity;
        this.fieldTypes = fieldTypes;
        this.fieldBuffer = (strategy == Strategy.FIXED_LENGTH_ROW) ? new long[arity] : null;
    }

    /**
     * Create an accessor for the given logical field types.
     *
     * @param fieldTypes the logical types of the RowData fields
     * @return accessor, or null if the types cannot be analyzed
     */
    public static RowDataKeyAccessor create(LogicalType[] fieldTypes) {
        if (fieldTypes == null || fieldTypes.length == 0) {
            return new RowDataKeyAccessor(Strategy.GENERIC, 0, fieldTypes);
        }

        int arity = fieldTypes.length;

        // Single-field unwrap optimization
        if (arity == 1) {
            Strategy s = singleFieldStrategy(fieldTypes[0]);
            return new RowDataKeyAccessor(s, 1, fieldTypes);
        }

        // Multi-field: check if all fields are fixed-length (can be stored as long)
        boolean allFixed = true;
        for (LogicalType t : fieldTypes) {
            if (!isFixedLengthAsLong(t)) {
                allFixed = false;
                break;
            }
        }

        if (allFixed) {
            return new RowDataKeyAccessor(Strategy.FIXED_LENGTH_ROW, arity, fieldTypes);
        }

        return new RowDataKeyAccessor(Strategy.GENERIC, arity, fieldTypes);
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public int getArity() {
        return arity;
    }

    public LogicalType[] getFieldTypes() {
        return fieldTypes;
    }

    // ========== Hot-path extraction methods ==========

    /** Extract single long value from RowData (SINGLE_LONG strategy). */
    public long extractSingleLong(Object key) {
        return ((RowData) key).getLong(0);
    }

    /** Extract single int value from RowData (SINGLE_INT strategy). */
    public int extractSingleInt(Object key) {
        return ((RowData) key).getInt(0);
    }

    /** Extract single double value from RowData (SINGLE_DOUBLE strategy). */
    public double extractSingleDouble(Object key) {
        return ((RowData) key).getDouble(0);
    }

    /**
     * Extract all fixed-length fields as long[] (FIXED_LENGTH_ROW strategy).
     * Returns the internal reusable buffer — caller must not retain reference.
     */
    public long[] extractFixedFields(Object key) {
        RowData row = (RowData) key;
        for (int i = 0; i < arity; i++) {
            fieldBuffer[i] = extractFieldAsLong(row, i, fieldTypes[i]);
        }
        return fieldBuffer;
    }

    /**
     * Reconstruct a RowData from a single long value (for value() return path).
     */
    public RowData reconstructFromLong(long value) {
        org.apache.flink.table.data.GenericRowData row =
                new org.apache.flink.table.data.GenericRowData(1);
        setFieldFromLong(row, 0, value, fieldTypes[0]);
        return row;
    }

    /**
     * Reconstruct a RowData from a long[] (for value() return path).
     */
    public RowData reconstructFromLongArray(long[] fields) {
        org.apache.flink.table.data.GenericRowData row =
                new org.apache.flink.table.data.GenericRowData(fields.length);
        for (int i = 0; i < fields.length; i++) {
            setFieldFromLong(row, i, fields[i], fieldTypes[i]);
        }
        return row;
    }

    // ========== Internal helpers ==========

    private static Strategy singleFieldStrategy(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_DAY_TIME:
                return Strategy.SINGLE_LONG;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case SMALLINT:
            case TINYINT:
                return Strategy.SINGLE_INT;
            case DOUBLE:
                return Strategy.SINGLE_DOUBLE;
            case FLOAT:
                return Strategy.SINGLE_FLOAT;
            case BOOLEAN:
                return Strategy.SINGLE_BOOLEAN;
            default:
                return Strategy.GENERIC;
        }
    }

    /**
     * Check if a LogicalType can be stored in a single int64_t slot.
     * This covers all fixed-length BinaryRowData field types.
     */
    static boolean isFixedLengthAsLong(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    /**
     * Extract a field from RowData as a long, using the appropriate typed getter.
     * For sub-long types (int, short, byte, float, boolean), values are widened to long.
     */
    private static long extractFieldAsLong(RowData row, int pos, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return row.getLong(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return row.getInt(pos);
            case SMALLINT:
                return row.getShort(pos);
            case TINYINT:
                return row.getByte(pos);
            case FLOAT:
                return Float.floatToIntBits(row.getFloat(pos));
            case DOUBLE:
                return Double.doubleToLongBits(row.getDouble(pos));
            case BOOLEAN:
                return row.getBoolean(pos) ? 1L : 0L;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Compact timestamp (precision <= 3): stored as millis (long)
                return row.getTimestamp(pos, 3).getMillisecond();
            default:
                throw new UnsupportedOperationException(
                        "Cannot extract " + type.getTypeRoot() + " as long");
        }
    }

    /**
     * Set a field on GenericRowData from a long value, reversing the extraction.
     */
    private static void setFieldFromLong(
            org.apache.flink.table.data.GenericRowData row,
            int pos, long value, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case INTERVAL_DAY_TIME:
                row.setField(pos, value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                row.setField(pos, (int) value);
                break;
            case SMALLINT:
                row.setField(pos, (short) value);
                break;
            case TINYINT:
                row.setField(pos, (byte) value);
                break;
            case FLOAT:
                row.setField(pos, Float.intBitsToFloat((int) value));
                break;
            case DOUBLE:
                row.setField(pos, Double.longBitsToDouble(value));
                break;
            case BOOLEAN:
                row.setField(pos, value != 0);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                row.setField(pos,
                        org.apache.flink.table.data.TimestampData.fromEpochMillis(value));
                break;
            default:
                throw new UnsupportedOperationException(
                        "Cannot reconstruct " + type.getTypeRoot() + " from long");
        }
    }

    // ========== BinaryRowData zero-serialization ==========

    /**
     * Extract raw bytes from a RowData value, bypassing Flink's TypeSerializer entirely.
     * If the value is BinaryRowData and its backing array is exactly sized, returns the
     * backing array directly (TRUE zero-copy). Otherwise copies the relevant slice.
     * Returns null if value is not BinaryRowData (caller should fall back to serialization).
     *
     * @param value a RowData instance (expected to be BinaryRowData at runtime)
     * @return raw binary representation of the RowData, or null if not BinaryRowData
     */
    public static byte[] extractBinaryRowDataBytes(Object value) {
        if (!(value instanceof BinaryRowData)) {
            return null;
        }
        BinaryRowData row = (BinaryRowData) value;
        org.apache.flink.core.memory.MemorySegment seg = row.getSegments()[0];
        byte[] backing = seg.getArray();
        int offset = row.getOffset();
        int size = row.getSizeInBytes();
        if (backing != null) {
            // On-heap: direct array access
            if (offset == 0 && size == backing.length) {
                return backing;  // zero-copy: reuse backing array directly
            }
            byte[] result = new byte[size];
            System.arraycopy(backing, offset, result, 0, size);
            return result;
        }
        // Off-heap (from zero-copy read): copy out via MemorySegment.get()
        byte[] result = new byte[size];
        seg.get(offset, result, 0, size);
        return result;
    }

    /**
     * Produce RowDataSerializer-compatible bytes for a BinaryRowData value:
     * [4-byte big-endian size][raw BinaryRowData bytes].
     * This format is compatible with entries()/list() bulk deserialization.
     * Returns null if value is not BinaryRowData.
     */
    public static byte[] extractBinaryRowDataBytesCompat(Object value) {
        if (!(value instanceof BinaryRowData)) {
            return null;
        }
        BinaryRowData row = (BinaryRowData) value;
        org.apache.flink.core.memory.MemorySegment seg = row.getSegments()[0];
        byte[] backing = seg.getArray();
        int offset = row.getOffset();
        int size = row.getSizeInBytes();
        byte[] result = new byte[4 + size];
        result[0] = (byte) (size >>> 24);
        result[1] = (byte) (size >>> 16);
        result[2] = (byte) (size >>> 8);
        result[3] = (byte) size;
        if (backing != null) {
            System.arraycopy(backing, offset, result, 4, size);
        } else {
            // Off-heap (from zero-copy read)
            seg.get(offset, result, 4, size);
        }
        return result;
    }

    /**
     * Deserialize a BinaryRowData from RowDataSerializer-compatible bytes:
     * [4-byte big-endian size][raw bytes]. Shares the byte array — zero-copy.
     */
    public static BinaryRowData wrapBinaryRowDataCompat(byte[] bytes, int arity) {
        int size = ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) |
                   ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        BinaryRowData row = new BinaryRowData(arity);
        row.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(bytes), 4, size);
        return row;
    }

    /**
     * Wrap raw bytes as a BinaryRowData, bypassing Flink's TypeSerializer.deserialize().
     * The returned BinaryRowData directly wraps the input byte array (zero-copy).
     *
     * @param bytes raw binary representation (as stored in C++ engine)
     * @param arity number of fields in the RowData
     * @return BinaryRowData wrapping the bytes
     */
    public static BinaryRowData wrapBinaryRowData(byte[] bytes, int arity) {
        BinaryRowData row = new BinaryRowData(arity);
        row.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(bytes), 0, bytes.length);
        return row;
    }
}
