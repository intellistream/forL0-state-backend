package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ForL0 implementation of {@link InternalValueState}.
 *
 * <p>JNI thin shell — all state storage is in the C++ engine. Each method
 * forwards to a single {@link NativeEngine} JNI call.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of value
 */
public class ForL0ValueState<K, N, V>
        implements InternalValueState<K, N, V>, LongValueStateAddAndGet {

    private final long stateHandle;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> valueSerializer;
    private V defaultValue;
    private N currentNamespace;

    /** Type IDs for dispatch — determined once at construction. */
    private final int keyTypeId;
    private final int valueTypeId;
    private final boolean voidNamespace;
    private final boolean isTimeWindowNs;

    /**
     * RowData key accessor — non-null when key is RowData and can use fast path.
     * Handles both single-field unwrap and multi-field FixedLengthRow extraction.
     */
    private final RowDataKeyAccessor rowDataKeyAccessor;
    /** True when key is RowData (even if typeId maps to INT64/INT32 via unwrap). */
    private final boolean isRowDataKey;

    /**
     * RowData value accessor — non-null when value is RowData and can use fast path.
     */
    private final RowDataKeyAccessor rowDataValueAccessor;
    /** True when value is RowData. */
    private final boolean isRowDataValue;

    /** Reusable serialization buffers (single-threaded access). */
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(128);

    /** Pre-allocated buffer for combined get-or-null JNI calls (single hash lookup). */
    private final long readResultAddress;
    /** Pre-allocated buffer for zero-copy native pointer return [address, size]. */
    /** Arity of RowData value (for BinaryRowData reconstruction). */
    private final int rowDataValueArity;
    private final BinaryRowData longRowBuffer;

    /** Pre-computed dispatch strategy — eliminates multi-level if chains on hot path. */
    private enum KeyNsStrategy {
        LONG_VOID,           // voidNamespace, key=Long
        INT_VOID,            // voidNamespace, key=Int
        ROWDATA_LONG_VOID,   // voidNamespace, RowData key → extractSingleLong
        ROWDATA_INT_VOID,    // voidNamespace, RowData key → extractSingleInt
        ROWDATA_FIXED2_VOID, // voidNamespace, RowData key → two fixed fields without JNI array access
        ROWDATA_FIXED_VOID,  // voidNamespace, RowData key → extractFixedFields
        LONG_NS,             // Long window/slice namespace, numeric key
        LONG_TW,             // TimeWindow namespace, key=Long
        SQL_WINDOW,          // SQL TimeWindow, legacy serialized checkpoint key
        GENERIC              // fallback (generic namespace / unsupported key type)
    }
    private final KeyNsStrategy keyNsStrategy;
    private final MemorySegment serializedCacheSegment;
    private final long serializedCacheAddress;
    private BinaryRowData reusableNativeRow;
    private boolean nativeViewInUse;

    ForL0ValueState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.stateHandle = stateHandle;
        this.readResultAddress = NativeEngine.stateReadResultAddress(stateHandle);
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        this.valueTypeId = TypeAnalyzer.getTypeId(valueSerializer);
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);

        // RowData key detection
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey
                ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;

        // RowData value detection
        this.isRowDataValue = TypeAnalyzer.isRowDataSerializer(valueSerializer);
        this.rowDataValueAccessor = isRowDataValue
                ? TypeAnalyzer.createRowDataKeyAccessor(valueSerializer) : null;
        this.rowDataValueArity = isRowDataValue ? rowDataValueAccessor.getArity() : 0;
        long[] cacheRange = isRowDataValue ? NativeEngine.serializedCacheMemoryRange(stateHandle) : null;
        this.serializedCacheAddress = cacheRange == null ? 0L : cacheRange[0];
        this.serializedCacheSegment = cacheRange == null ? null
                : MemorySegmentBridge.wrapNativeAddress(cacheRange[0], (int) cacheRange[1]);

        // Pre-compute dispatch strategy (O(1) switch vs multi-level if chains)
        if (isRowDataKey && voidNamespace && rowDataKeyAccessor != null) {
            switch (rowDataKeyAccessor.getStrategy()) {
                case SINGLE_LONG: this.keyNsStrategy = KeyNsStrategy.ROWDATA_LONG_VOID; break;
                case SINGLE_INT:  this.keyNsStrategy = KeyNsStrategy.ROWDATA_INT_VOID;  break;
                case FIXED_LENGTH_ROW:
                    this.keyNsStrategy = rowDataKeyAccessor.getArity() == 2
                            ? KeyNsStrategy.ROWDATA_FIXED2_VOID
                            : KeyNsStrategy.ROWDATA_FIXED_VOID;
                    break;
                default: this.keyNsStrategy = KeyNsStrategy.GENERIC; break;
            }
        } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_VOID;
        } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT32) {
            this.keyNsStrategy = KeyNsStrategy.INT_VOID;
        } else if (namespaceSerializer instanceof org.apache.flink.api.common.typeutils.base.LongSerializer
                && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_NS;
        } else if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_TW;
        } else if (TypeAnalyzer.isSqlTimeWindowSerializer(namespaceSerializer) && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.SQL_WINDOW;
        } else {
            this.keyNsStrategy = KeyNsStrategy.GENERIC;
        }
        if (keyNsStrategy == KeyNsStrategy.SQL_WINDOW && rowDataValueArity == 1
                && rowDataValueAccessor.getFieldTypes()[0].getTypeRoot() == LogicalTypeRoot.BIGINT) {
            this.longRowBuffer = new BinaryRowData(1);
            this.longRowBuffer.pointTo(new MemorySegment[]{MemorySegmentFactory.wrapOffHeapMemory(ByteBuffer.allocateDirect(16))}, 0, 16);
        } else {
            this.longRowBuffer = null;
        }
    }

    @Override
    public V value() {
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        try {
            if (keyNsStrategy == KeyNsStrategy.LONG_NS) {
                return valueWithLongNamespace(keyAsLong(key), keyGroup, (Long) currentNamespace, longNamespaceKeyHeader(key));
            }
            if (keyNsStrategy == KeyNsStrategy.SQL_WINDOW) {
                return valueWithSqlWindow(key, keyGroup);
            }
            return valueOtherStrategies(key, keyGroup);
        } catch (IOException e) {
            throw new RuntimeException("Failed to access ValueState", e);
        }
    }

    private V valueWithSqlWindow(K key, int keyGroup) throws IOException {
        long start = TypeAnalyzer.windowStart(currentNamespace), end = TypeAnalyzer.windowEnd(currentNamespace);
        long numeric = keyAsLong(key), header = longNamespaceKeyHeader(key);
        if (isRowDataValue) return NativeEngine.valueGetSqlWindowPtr(stateHandle, numeric, keyGroup, start, end, header)
                ? wrapNativePtr() : getDefaultValue();
        byte[] bytes = NativeEngine.valueGetSqlWindowBytes(stateHandle, numeric, keyGroup, start, end, header);
        return bytes == null ? getDefaultValue() : deserializeValue(bytes);
    }

    private V valueOtherStrategies(K key, int keyGroup) throws IOException {
        switch (keyNsStrategy) {
            case LONG_VOID:
                return valueForLongKey((Long) key, keyGroup);
            case INT_VOID:
                return valueForIntKey((Integer) key, keyGroup);
            case ROWDATA_LONG_VOID:
                return valueForLongKey(rowDataKeyAccessor.extractSingleLong(key), keyGroup);
            case ROWDATA_INT_VOID:
                return valueForIntKey(rowDataKeyAccessor.extractSingleInt(key), keyGroup);
            case ROWDATA_FIXED2_VOID:
                return valueForFixedRow2Key(
                        rowDataKeyAccessor.extractFixedField(key, 0),
                        rowDataKeyAccessor.extractFixedField(key, 1),
                        keyGroup);
            case ROWDATA_FIXED_VOID:
                return valueForFixedRowKey(rowDataKeyAccessor.extractFixedFields(key), keyGroup);
            case LONG_TW: {
                TimeWindow tw = (TimeWindow) currentNamespace;
                return valueWithTimeWindow(isRowDataKey ? rowDataKeyAccessor.extractSingleLong(key) : (Long) key, keyGroup, tw.getStart(), tw.getEnd());
            }
            default: {
                byte[] keyBytes = serializeKey(key);
                if (isRowDataValue) {
                    return zeroCopyGetGeneric(keyBytes, keyGroup);
                }
                byte[] valueBytes = NativeEngine.valueGetGeneric(stateHandle, keyBytes, keyGroup);
                return valueBytes != null ? deserializeValue(valueBytes) : getDefaultValue();
            }
        }
    }

    /**
     * Fused read-modify-write fast path for long-valued counters.
     *
     * <p>Used by benchmarks and counter-like operators to avoid a separate get + put JNI roundtrip.
     */
    public long addAndGetLong(long delta) {
        if (valueTypeId != TypeAnalyzer.TYPE_INT64 || isRowDataValue) {
            throw new IllegalStateException("addAndGetLong requires non-RowData long ValueState");
        }

        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        switch (keyNsStrategy) {
            case LONG_VOID:
                return NativeEngine.valueAddAndGetLongLong(stateHandle, (Long) key, keyGroup, delta);
            case INT_VOID:
                return NativeEngine.valueAddAndGetIntLong(stateHandle, (Integer) key, keyGroup, delta);
            case ROWDATA_LONG_VOID:
                return NativeEngine.valueAddAndGetLongLong(
                        stateHandle,
                        rowDataKeyAccessor.extractSingleLong(key),
                        keyGroup,
                        delta);
            case ROWDATA_INT_VOID:
                return NativeEngine.valueAddAndGetIntLong(
                        stateHandle,
                        rowDataKeyAccessor.extractSingleInt(key),
                        keyGroup,
                        delta);
            case LONG_TW: {
                TimeWindow tw = (TimeWindow) currentNamespace;
                return NativeEngine.valueAddAndGetLongLongWithTW(
                        stateHandle,
                        (Long) key,
                        keyGroup,
                        tw.getStart(),
                        tw.getEnd(),
                        delta);
            }
            default:
                throw new IllegalStateException("addAndGetLong is only supported for primitive long/int key fast paths");
        }
    }

    private long keyAsLong(K key) {
        return isRowDataKey ? (((org.apache.flink.table.data.RowData) key).isNullAt(0) ? 0L
                : rowDataKeyAccessor.extractFixedField(key, 0)) : (Long) key;
    }

    private long longNamespaceKeyHeader(K key) {
        if (!isRowDataKey) return 0;
        org.apache.flink.table.data.RowData row = (org.apache.flink.table.data.RowData) key;
        return row.getRowKind().toByteValue() | (row.isNullAt(0) ? 256L : 0L);
    }

    @SuppressWarnings("unchecked")
    private V valueWithLongNamespace(long key, int group, long ns, long header) throws IOException {
        if (!isRowDataValue && (valueTypeId == TypeAnalyzer.TYPE_INT64 || valueTypeId == TypeAnalyzer.TYPE_INT32
                || valueTypeId == TypeAnalyzer.TYPE_FLOAT64)) {
            if (!NativeEngine.valueGetNumberLongNs(stateHandle, key, group, ns, header, null)) return getDefaultValue();
            long bits = readLongResult();
            if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) return (V) Double.valueOf(Double.longBitsToDouble(bits));
            if (valueTypeId == TypeAnalyzer.TYPE_INT32) return (V) Integer.valueOf((int) bits);
            return (V) Long.valueOf(bits);
        }
        if (isRowDataValue) {
            return NativeEngine.valueGetBlobPtrLongNs(stateHandle, key, group, ns, header)
                    ? wrapNativePtr() : getDefaultValue();
        }
        byte[] bytes = NativeEngine.valueGetBlobLongNs(stateHandle, key, group, ns, header);
        return bytes == null ? getDefaultValue() : deserializeValue(bytes);
    }

    private void updateWithLongNamespace(long key, int group, long ns, long header, V value) throws IOException {
        if (!isRowDataValue && (valueTypeId == TypeAnalyzer.TYPE_INT64 || valueTypeId == TypeAnalyzer.TYPE_INT32
                || valueTypeId == TypeAnalyzer.TYPE_FLOAT64)) {
            long bits = valueTypeId == TypeAnalyzer.TYPE_FLOAT64 ? Double.doubleToRawLongBits((Double) value)
                    : ((Number) value).longValue();
            NativeEngine.valuePutNumberLongNs(stateHandle, key, group, ns, header, bits);
        } else if (isRowDataValue) {
            BinaryRowData row = TypeAnalyzer.toBinaryRow(valueSerializer, (org.apache.flink.table.data.RowData) value);
            MemorySegment[] segments = row.getSegments();
            if (segments.length == 1 && row.getOffset() <= segments[0].size() - row.getSizeInBytes()) {
                MemorySegment segment = segments[0];
                if (segment.isOffHeap()) {
                    NativeEngine.valuePutBlobAddressLongNs(stateHandle, key, group, ns, header,
                            row, segment.getAddress() + row.getOffset(), row.getSizeInBytes());
                } else {
                    NativeEngine.valuePutBlobRangeLongNs(stateHandle, key, group, ns, header,
                            segment.getArray(), row.getOffset(), row.getSizeInBytes());
                }
            } else {
                // Rare segmented rows are flattened once using Flink's own copier.
                BinaryRowData flat = row.copy();
                NativeEngine.valuePutBlobRangeLongNs(stateHandle, key, group, ns, header,
                        flat.getSegments()[0].getArray(), flat.getOffset(), flat.getSizeInBytes());
            }
        } else NativeEngine.valuePutBlobLongNs(stateHandle, key, group, ns, header, serializeValue(value));
    }

    private void updateSqlWindow(long key, int group, long header, V value) throws IOException {
        long start = TypeAnalyzer.windowStart(currentNamespace), end = TypeAnalyzer.windowEnd(currentNamespace);
        if (!isRowDataValue) {
            byte[] bytes = serializeValue(value);
            NativeEngine.valuePutSqlWindowRange(stateHandle, key, group, start, end, header, bytes, 0, bytes.length);
            return;
        }
        BinaryRowData row;
        if (longRowBuffer != null && value instanceof GenericRowData) {
            GenericRowData input = (GenericRowData) value;
            row = longRowBuffer;
            row.setRowKind(input.getRowKind());
            if (input.isNullAt(0)) row.setNullAt(0);
            else row.setLong(0, input.getLong(0));
        } else {
            row = TypeAnalyzer.toBinaryRow(valueSerializer, (org.apache.flink.table.data.RowData) value);
        }
        if (row.getSegments().length != 1 || row.getOffset() > row.getSegments()[0].size() - row.getSizeInBytes()) row = row.copy();
        MemorySegment segment = row.getSegments()[0];
        if (segment.isOffHeap()) {
            NativeEngine.valuePutSqlWindowAddress(stateHandle, key, group, start, end, header,
                    row, segment.getAddress() + row.getOffset(), row.getSizeInBytes());
        } else {
            NativeEngine.valuePutSqlWindowRange(stateHandle, key, group, start, end, header,
                    segment.getArray(), row.getOffset(), row.getSizeInBytes());
        }
    }

    /** Get value for a long key extracted from RowData. */
    @SuppressWarnings("unchecked")
    private V valueForLongKey(long k, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Long.valueOf(readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, keyGroup, null)) {
                return getDefaultValue();
            }
            return (V) Integer.valueOf((int) readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetLongDoubleSafe(stateHandle, k, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Double.valueOf(Double.longBitsToDouble(readLongResult()));
        }
        // String/bytes value
        if (isRowDataValue) {
            return zeroCopyGetLong(k, keyGroup);
        }
        byte[] valueBytes = NativeEngine.valueGetLongString(stateHandle, k, keyGroup);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
    }

    /** Get value for an int key (DataStream Integer key or RowData SINGLE_INT). */
    @SuppressWarnings("unchecked")
    private V valueForIntKey(int k, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetIntLongSafe(stateHandle, k, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Long.valueOf(readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetIntDoubleSafe(stateHandle, k, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Double.valueOf(Double.longBitsToDouble(readLongResult()));
        }
        // String/bytes value
        byte[] valueBytes = NativeEngine.valueGetIntString(stateHandle, k, keyGroup);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
    }

    /** Get value for a FixedLengthRow key. */
    @SuppressWarnings("unchecked")
    private V valueForFixedRow2Key(long key0, long key1, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetFixedRow2LongSafe(stateHandle, key0, key1, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Long.valueOf(readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetFixedRow2DoubleSafe(stateHandle, key0, key1, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Double.valueOf(Double.longBitsToDouble(readLongResult()));
        }
        if (isRowDataValue) {
            if (NativeEngine.valueGetFixedRow2GenericPtr(stateHandle, key0, key1, keyGroup, null)) {
                return wrapNativePtr();
            }
            return getDefaultValue();
        }
        byte[] valueBytes = NativeEngine.valueGetFixedRow2Generic(stateHandle, key0, key1, keyGroup);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
    }

    /** Get value for a FixedLengthRow key. */
    @SuppressWarnings("unchecked")
    private V valueForFixedRowKey(long[] fields, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetFixedRowLongSafe(stateHandle, fields, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Long.valueOf(readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetFixedRowDoubleSafe(stateHandle, fields, keyGroup, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Double.valueOf(Double.longBitsToDouble(readLongResult()));
        }
        // String/bytes value
        if (isRowDataValue) {
            if (NativeEngine.valueGetFixedRowGenericPtr(stateHandle, fields, keyGroup, null)) {
                return wrapNativePtr();
            }
            return getDefaultValue();
        }
        byte[] valueBytes = NativeEngine.valueGetFixedRowGeneric(stateHandle, fields, keyGroup);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
    }

    /** Get value with TimeWindow namespace — long key dispatch. */
    @SuppressWarnings("unchecked")
    private V valueWithTimeWindow(long k, int keyGroup, long nsStart, long nsEnd) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Long.valueOf(readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            if (!NativeEngine.valueGetLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Integer.valueOf((int) readLongResult());
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetLongDoubleWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, null)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(readLongResult());
            }
            return (V) Double.valueOf(Double.longBitsToDouble(readLongResult()));
        }
        if (isRowDataValue) {
            return zeroCopyGetLongWithTimeWindow(k, keyGroup, nsStart, nsEnd);
        }
        byte[] valueBytes = NativeEngine.valueGetLongStringWithTW(stateHandle, k, keyGroup, nsStart, nsEnd);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
    }

    /** Put value with TimeWindow namespace — long key dispatch. */
    private void updateWithTimeWindow(long k, int keyGroup, long nsStart, long nsEnd, V value) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            long raw = isRowDataValue ? rowDataValueAccessor.extractSingleLong(value) : (Long) value;
            NativeEngine.valuePutLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            long raw = isRowDataValue ? rowDataValueAccessor.extractSingleLong(value) : (long) (Integer) value;
            NativeEngine.valuePutLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            double raw = isRowDataValue
                    ? Double.longBitsToDouble(rowDataValueAccessor.extractSingleLong(value))
                    : (Double) value;
            NativeEngine.valuePutLongDoubleWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, raw);
            return;
        }
        byte[] valueBytes = serializeValue(value);
        NativeEngine.valuePutLongStringWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, valueBytes);
    }

    @Override
    public void update(V value) {
        if (value == null) { clear(); return; }
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        try {
            if (keyNsStrategy == KeyNsStrategy.LONG_NS) {
                updateWithLongNamespace(keyAsLong(key), keyGroup, (Long) currentNamespace, longNamespaceKeyHeader(key), value);
            } else if (keyNsStrategy == KeyNsStrategy.SQL_WINDOW) {
                updateSqlWindow(keyAsLong(key), keyGroup, longNamespaceKeyHeader(key), value);
            } else {
                updateOtherStrategies(key, keyGroup, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update ValueState", e);
        } finally {
            nativeViewInUse = false;
        }
    }

    private void updateOtherStrategies(K key, int keyGroup, V value) throws IOException {
        switch (keyNsStrategy) {
            case LONG_VOID:
                updateForLongKey((Long) key, keyGroup, value);
                return;
            case INT_VOID:
                updateForIntKey((Integer) key, keyGroup, value);
                return;
            case ROWDATA_LONG_VOID:
                updateForLongKey(rowDataKeyAccessor.extractSingleLong(key), keyGroup, value);
                return;
            case ROWDATA_INT_VOID:
                updateForIntKey(rowDataKeyAccessor.extractSingleInt(key), keyGroup, value);
                return;
            case ROWDATA_FIXED2_VOID:
                updateForFixedRow2Key(
                        rowDataKeyAccessor.extractFixedField(key, 0),
                        rowDataKeyAccessor.extractFixedField(key, 1),
                        keyGroup,
                        value);
                return;
            case ROWDATA_FIXED_VOID:
                updateForFixedRowKey(rowDataKeyAccessor.extractFixedFields(key), keyGroup, value);
                return;
            case LONG_TW: {
                TimeWindow tw = (TimeWindow) currentNamespace;
                updateWithTimeWindow(isRowDataKey ? rowDataKeyAccessor.extractSingleLong(key) : (Long) key, keyGroup, tw.getStart(), tw.getEnd(), value);
                return;
            }
            default: {
                byte[] keyBytes = serializeKey(key);
                byte[] valueBytes = serializeValue(value);
                NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, valueBytes);
            }
        }
    }

    /** Put value for a long key extracted from RowData. */
    private void updateForLongKey(long k, int keyGroup, V value) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            long raw = isRowDataValue
                    ? rowDataValueAccessor.extractSingleLong(value)
                    : (Long) value;
            NativeEngine.valuePutLongLong(stateHandle, k, keyGroup, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            NativeEngine.valuePutLongInt(stateHandle, k, keyGroup, (Integer) value);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            double raw = isRowDataValue
                    ? Double.longBitsToDouble(rowDataValueAccessor.extractSingleLong(value))
                    : (Double) value;
            NativeEngine.valuePutLongDouble(stateHandle, k, keyGroup, raw);
            return;
        }
        byte[] valueBytes = serializeValue(value);
        NativeEngine.valuePutLongString(stateHandle, k, keyGroup, valueBytes);
    }

    /** Put value for an int key (DataStream Integer key or RowData SINGLE_INT). */
    private void updateForIntKey(int k, int keyGroup, V value) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            long raw = isRowDataValue
                    ? rowDataValueAccessor.extractSingleLong(value)
                    : (Long) value;
            NativeEngine.valuePutIntLong(stateHandle, k, keyGroup, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            double raw = isRowDataValue
                    ? Double.longBitsToDouble(rowDataValueAccessor.extractSingleLong(value))
                    : (Double) value;
            NativeEngine.valuePutIntDouble(stateHandle, k, keyGroup, raw);
            return;
        }
        byte[] valueBytes = serializeValue(value);
        NativeEngine.valuePutIntString(stateHandle, k, keyGroup, valueBytes);
    }

    /** Put value for a FixedLengthRow key. */
    private void updateForFixedRow2Key(long key0, long key1, int keyGroup, V value) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            long raw = isRowDataValue
                    ? rowDataValueAccessor.extractSingleLong(value)
                    : (Long) value;
            NativeEngine.valuePutFixedRow2Long(stateHandle, key0, key1, keyGroup, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            double raw = isRowDataValue
                    ? Double.longBitsToDouble(rowDataValueAccessor.extractSingleLong(value))
                    : (Double) value;
            NativeEngine.valuePutFixedRow2Double(stateHandle, key0, key1, keyGroup, raw);
            return;
        }
        byte[] valueBytes = serializeValue(value);
        NativeEngine.valuePutFixedRow2Generic(stateHandle, key0, key1, keyGroup, valueBytes);
    }

    /** Put value for a FixedLengthRow key. */
    private void updateForFixedRowKey(long[] fields, int keyGroup, V value) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            long raw = isRowDataValue
                    ? rowDataValueAccessor.extractSingleLong(value)
                    : (Long) value;
            NativeEngine.valuePutFixedRowLong(stateHandle, fields, keyGroup, raw);
            return;
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            double raw = isRowDataValue
                    ? Double.longBitsToDouble(rowDataValueAccessor.extractSingleLong(value))
                    : (Double) value;
            NativeEngine.valuePutFixedRowDouble(stateHandle, fields, keyGroup, raw);
            return;
        }
        byte[] valueBytes = serializeValue(value);
        NativeEngine.valuePutFixedRowGeneric(stateHandle, fields, keyGroup, valueBytes);
    }

    @Override
    public void clear() {
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        try {
            switch (keyNsStrategy) {
                case LONG_VOID:
                    NativeEngine.valueClearLong(stateHandle, (Long) key, keyGroup);
                    return;
                case INT_VOID:
                    NativeEngine.valueClearInt(stateHandle, (Integer) key, keyGroup);
                    return;
                case ROWDATA_LONG_VOID:
                    NativeEngine.valueClearLong(stateHandle, rowDataKeyAccessor.extractSingleLong(key), keyGroup);
                    return;
                case ROWDATA_INT_VOID:
                    NativeEngine.valueClearInt(stateHandle, rowDataKeyAccessor.extractSingleInt(key), keyGroup);
                    return;
                case ROWDATA_FIXED2_VOID:
                    NativeEngine.valueClearFixedRow2(
                            stateHandle,
                            rowDataKeyAccessor.extractFixedField(key, 0),
                            rowDataKeyAccessor.extractFixedField(key, 1),
                            keyGroup);
                    return;
                case ROWDATA_FIXED_VOID:
                    NativeEngine.valueClearFixedRow(stateHandle, rowDataKeyAccessor.extractFixedFields(key), keyGroup);
                    return;
                case LONG_NS:
                    NativeEngine.valueClearLongNs(stateHandle, keyAsLong(key), keyGroup, (Long) currentNamespace, longNamespaceKeyHeader(key));
                    return;
                case SQL_WINDOW:
                    NativeEngine.valueClearSqlWindow(stateHandle, keyAsLong(key), keyGroup,
                            TypeAnalyzer.windowStart(currentNamespace), TypeAnalyzer.windowEnd(currentNamespace), longNamespaceKeyHeader(key));
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.valueClearWithTW(stateHandle, isRowDataKey ? rowDataKeyAccessor.extractSingleLong(key) : (Long) key, keyGroup, tw.getStart(), tw.getEnd());
                    return;
                }
                default: {
                    byte[] keyBytes = serializeKey(key);
                    NativeEngine.valueClearGeneric(stateHandle, keyBytes, keyGroup);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear ValueState", e);
        } finally {
            nativeViewInUse = false;
        }
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    // ========== Zero-copy read helpers (BinaryRowData → native pointer) ==========

    /** Zero-copy read via Long key: returns value or defaultValue. */
    private V zeroCopyGetLong(long k, int keyGroup) {
        if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, null)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    /** Zero-copy read via Long key + TimeWindow namespace: returns value or defaultValue. */
    private V zeroCopyGetLongWithTimeWindow(long k, int keyGroup, long nsStart, long nsEnd) {
        if (NativeEngine.valueGetLongStringPtrWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, null)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    /** Zero-copy read via Generic key: returns value or defaultValue. */
    private V zeroCopyGetGeneric(byte[] keyBytes, int keyGroup) {
        if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyGroup, null)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    /**
     * Wrap native pointer [address, size] as BinaryRowData via off-heap MemorySegment.
     *
     * <p><b>Lifetime safety:</b> The returned BinaryRowData borrows C++ memory
     * from SwissTable or the owned L0 cache. The view is valid only until
     * the next write operation (put/remove) on the same StateTable, which may trigger
     * rehash or overwrite the slot. Callers must consume or copy the returned value
     * before performing any state mutation. Its wrapper may also be reused after
     * that mutation; consecutive reads have independent wrappers.
     *
     * <p>During COW snapshot iteration, no rehash is triggered, so checkpoint reads
     * via this path are safe.
     */
    private long readLongResult() {
        return MemorySegmentBridge.readNativeLong(readResultAddress);
    }

    private V wrapNativePtr() {
        return wrapNativePtr(readLongResult(),
                (int) MemorySegmentBridge.readNativeLong(readResultAddress + 8));
    }

    @SuppressWarnings("unchecked")
    private V wrapNativePtr(long address, int size) {
        long cacheOffset = address - serializedCacheAddress;
        boolean inCache = serializedCacheSegment != null && cacheOffset >= 0
                && cacheOffset <= serializedCacheSegment.size() - size;
        MemorySegment seg = inCache ? serializedCacheSegment
                : MemorySegmentBridge.wrapNativeAddress(address, size);
        // Native views expire at the next mutation. Reuse the wrapper only
        // after that boundary; consecutive reads still receive independent
        // wrappers and may retain their views until a state mutation.
        BinaryRowData row = !nativeViewInUse && reusableNativeRow != null
                ? reusableNativeRow : new BinaryRowData(rowDataValueArity);
        MemorySegment[] segments = row.getSegments();
        if (segments == null) segments = new MemorySegment[]{seg};
        else segments[0] = seg;
        row.pointTo(segments, inCache ? (int) cacheOffset : 0, size);
        reusableNativeRow = row;
        nativeViewInUse = true;
        return (V) row;
    }

    // ========== Serialization helpers (generic path) ==========

    private byte[] serializeKey(K key) throws IOException {
        keyOut.clear();
        if (!voidNamespace) {
            ((TypeSerializer<N>) namespaceSerializer).serialize(currentNamespace, keyOut);
        }
        keySerializer.serialize(key, keyOut);
        return keyOut.getCopyOfBuffer();
    }

    private byte[] serializeValue(V value) throws IOException {
        if (isRowDataValue) {
            BinaryRowData row = TypeAnalyzer.toBinaryRow(valueSerializer, (org.apache.flink.table.data.RowData) value);
            if (row.getSegments().length != 1 || row.getOffset() > row.getSegments()[0].size() - row.getSizeInBytes()) row = row.copy();
            return RowDataKeyAccessor.extractBinaryRowDataBytes(row);
        }
        valueOut.clear();
        valueSerializer.serialize(value, valueOut);
        return valueOut.getCopyOfBuffer();
    }

    @SuppressWarnings("unchecked")
    private V deserializeValue(byte[] bytes) throws IOException {
        if (isRowDataValue) {
            // zero-ser: wrap raw bytes as BinaryRowData directly
            return (V) RowDataKeyAccessor.wrapBinaryRowData(bytes, rowDataValueAccessor.getArity());
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        return valueSerializer.deserialize(in);
    }

    private V getDefaultValue() {
        return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            long stateHandle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            ForL0KeyedStateBackend<K> backend) {
        return (IS) new ForL0ValueState<>(
                stateHandle,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                namespaceSerializer,
                stateSerializer,
                stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            IS existingState) {
        return (IS) ((ForL0ValueState<K, N, SV>) existingState)
                .setNamespaceSerializer(namespaceSerializer)
                .setValueSerializer(stateSerializer)
                .setDefaultValue(stateDesc.getDefaultValue());
    }

    ForL0ValueState<K, N, V> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0ValueState<K, N, V> setValueSerializer(TypeSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0ValueState<K, N, V> setDefaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
