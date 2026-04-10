package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.io.IOException;

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
public class ForL0ValueState<K, N, V> implements InternalValueState<K, N, V> {

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
    private final long[] primitiveBuf = new long[1];
    /** Pre-allocated buffer for zero-copy native pointer return [address, size]. */
    private final long[] nativePtrBuf;
    /** Arity of RowData value (for BinaryRowData reconstruction). */
    private final int rowDataValueArity;

    /** Pre-computed dispatch strategy — eliminates multi-level if chains on hot path. */
    private enum KeyNsStrategy {
        LONG_VOID,           // voidNamespace, key=Long
        INT_VOID,            // voidNamespace, key=Int
        ROWDATA_LONG_VOID,   // voidNamespace, RowData key → extractSingleLong
        ROWDATA_INT_VOID,    // voidNamespace, RowData key → extractSingleInt
        ROWDATA_FIXED_VOID,  // voidNamespace, RowData key → extractFixedFields
        LONG_TW,             // TimeWindow namespace, key=Long
        GENERIC              // fallback (generic namespace / unsupported key type)
    }
    private final KeyNsStrategy keyNsStrategy;

    ForL0ValueState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.stateHandle = stateHandle;
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
        this.nativePtrBuf = isRowDataValue ? new long[2] : null;
        this.rowDataValueArity = isRowDataValue ? rowDataValueAccessor.getArity() : 0;

        // Pre-compute dispatch strategy (O(1) switch vs multi-level if chains)
        if (isRowDataKey && voidNamespace && rowDataKeyAccessor != null) {
            switch (rowDataKeyAccessor.getStrategy()) {
                case SINGLE_LONG: this.keyNsStrategy = KeyNsStrategy.ROWDATA_LONG_VOID; break;
                case SINGLE_INT:  this.keyNsStrategy = KeyNsStrategy.ROWDATA_INT_VOID;  break;
                case FIXED_LENGTH_ROW: this.keyNsStrategy = KeyNsStrategy.ROWDATA_FIXED_VOID; break;
                default: this.keyNsStrategy = KeyNsStrategy.GENERIC; break;
            }
        } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_VOID;
        } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT32) {
            this.keyNsStrategy = KeyNsStrategy.INT_VOID;
        } else if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_TW;
        } else {
            this.keyNsStrategy = KeyNsStrategy.GENERIC;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V value() {
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        try {
            switch (keyNsStrategy) {
                case LONG_VOID:
                    return valueForLongKey((Long) key, keyGroup);
                case INT_VOID:
                    return valueForIntKey((Integer) key, keyGroup);
                case ROWDATA_LONG_VOID:
                    return valueForLongKey(rowDataKeyAccessor.extractSingleLong(key), keyGroup);
                case ROWDATA_INT_VOID:
                    return valueForIntKey(rowDataKeyAccessor.extractSingleInt(key), keyGroup);
                case ROWDATA_FIXED_VOID:
                    return valueForFixedRowKey(rowDataKeyAccessor.extractFixedFields(key), keyGroup);
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    return valueWithTimeWindow((Long) key, keyGroup, tw.getStart(), tw.getEnd());
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
        } catch (IOException e) {
            throw new RuntimeException("Failed to access ValueState", e);
        }
    }

    /** Get value for a long key extracted from RowData. */
    @SuppressWarnings("unchecked")
    private V valueForLongKey(long k, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Long.valueOf(primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            return (V) Integer.valueOf((int) primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetLongDoubleSafe(stateHandle, k, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Double.valueOf(Double.longBitsToDouble(primitiveBuf[0]));
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
            if (!NativeEngine.valueGetIntLongSafe(stateHandle, k, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Long.valueOf(primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetIntDoubleSafe(stateHandle, k, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Double.valueOf(Double.longBitsToDouble(primitiveBuf[0]));
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
    private V valueForFixedRowKey(long[] fields, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueGetFixedRowLongSafe(stateHandle, fields, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Long.valueOf(primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetFixedRowDoubleSafe(stateHandle, fields, keyGroup, primitiveBuf)) {
                return getDefaultValue();
            }
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
            }
            return (V) Double.valueOf(Double.longBitsToDouble(primitiveBuf[0]));
        }
        // String/bytes value
        if (isRowDataValue) {
            if (NativeEngine.valueGetFixedRowGenericPtr(stateHandle, fields, keyGroup, nativePtrBuf)) {
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
            if (!NativeEngine.valueGetLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, primitiveBuf)) {
                return getDefaultValue();
            }
            return (V) Long.valueOf(primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_INT32) {
            if (!NativeEngine.valueGetLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, primitiveBuf)) {
                return getDefaultValue();
            }
            return (V) Integer.valueOf((int) primitiveBuf[0]);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueGetLongDoubleWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, primitiveBuf)) {
                return getDefaultValue();
            }
            return (V) Double.valueOf(Double.longBitsToDouble(primitiveBuf[0]));
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
        if (value == null) {
            clear();
            return;
        }
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        try {
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
                case ROWDATA_FIXED_VOID:
                    updateForFixedRowKey(rowDataKeyAccessor.extractFixedFields(key), keyGroup, value);
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    updateWithTimeWindow((Long) key, keyGroup, tw.getStart(), tw.getEnd(), value);
                    return;
                }
                default: {
                    byte[] keyBytes = serializeKey(key);
                    byte[] valueBytes = serializeValue(value);
                    NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, valueBytes);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update ValueState", e);
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
                case ROWDATA_FIXED_VOID:
                    NativeEngine.valueClearFixedRow(stateHandle, rowDataKeyAccessor.extractFixedFields(key), keyGroup);
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.valueClearWithTW(stateHandle, (Long) key, keyGroup, tw.getStart(), tw.getEnd());
                    return;
                }
                default: {
                    byte[] keyBytes = serializeKey(key);
                    NativeEngine.valueClearGeneric(stateHandle, keyBytes, keyGroup);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear ValueState", e);
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
    @SuppressWarnings("unchecked")
    private V zeroCopyGetLong(long k, int keyGroup) {
        if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    /** Zero-copy read via Generic key: returns value or defaultValue. */
    @SuppressWarnings("unchecked")
    private V zeroCopyGetGeneric(byte[] keyBytes, int keyGroup) {
        if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyGroup, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    /**
     * Wrap native pointer [address, size] as BinaryRowData via off-heap MemorySegment.
     *
     * <p><b>Lifetime safety:</b> The returned BinaryRowData references C++ memory
     * (std::string::data()) inside a SwissTable slot. The pointer is valid only until
     * the next write operation (put/remove) on the same StateTable, which may trigger
     * rehash or overwrite the slot. Callers must consume or copy the returned value
     * before performing any state mutation.
     *
     * <p>During COW snapshot iteration, no rehash is triggered, so checkpoint reads
     * via this path are safe.
     */
    @SuppressWarnings("unchecked")
    private V wrapNativePtr() {
        int size = (int) nativePtrBuf[1];
        MemorySegment seg = MemorySegmentBridge.wrapNativeAddress(nativePtrBuf[0], size);
        BinaryRowData row = new BinaryRowData(rowDataValueArity);
        row.pointTo(seg, 0, size);
        return (V) row;
    }

    // ========== Serialization helpers (generic path) ==========

    @SuppressWarnings("unchecked")
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
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytes(value);
            if (raw != null) return raw;  // zero-ser: raw BinaryRowData bytes
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
