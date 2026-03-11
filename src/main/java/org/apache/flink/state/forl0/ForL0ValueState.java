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

    /** Pre-allocated buffer for zero-copy native pointer return [address, size]. */
    private final long[] nativePtrBuf;
    /** Arity of RowData value (for BinaryRowData reconstruction). */
    private final int rowDataValueArity;

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
    }

    @SuppressWarnings("unchecked")
    @Override
    public V value() {
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        try {
            // ========== RowData key paths (zero-serialization) ==========
            if (isRowDataKey && voidNamespace && rowDataKeyAccessor != null) {
                return valueWithRowDataKey(key, keyGroup);
            }

            // ========== Primitive key fast paths ==========

            // Fast path: long key + long value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                if (!NativeEngine.valueContains(stateHandle, ((Long) key), keyGroup)) {
                    return getDefaultValue();
                }
                return (V) Long.valueOf(NativeEngine.valueGetLongLong(stateHandle, (Long) key, keyGroup));
            }
            // Fast path: long key + int value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT32) {
                if (!NativeEngine.valueContains(stateHandle, ((Long) key), keyGroup)) {
                    return getDefaultValue();
                }
                return (V) Integer.valueOf(NativeEngine.valueGetLongInt(stateHandle, (Long) key, keyGroup));
            }
            // Fast path: long key + double value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
                if (!NativeEngine.valueContains(stateHandle, ((Long) key), keyGroup)) {
                    return getDefaultValue();
                }
                return (V) Double.valueOf(NativeEngine.valueGetLongDouble(stateHandle, (Long) key, keyGroup));
            }
            // Fast path: long key + other value (String/BYTES/etc) (VoidNamespace only)
            // C++ stores as <int64_t, std::string> — must NOT fall to generic <string,string>
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                if (isRowDataValue) {
                    return zeroCopyGetLong((Long) key, keyGroup);
                }
                byte[] valueBytes = NativeEngine.valueGetLongString(stateHandle, (Long) key, keyGroup);
                if (valueBytes == null) {
                    return getDefaultValue();
                }
                return deserializeValue(valueBytes);
            }
            // Fast path: int key (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT32) {
                return valueForIntKey((Integer) key, keyGroup);
            }
            // Generic path (non-VoidNamespace or unsupported key type)
            byte[] keyBytes = serializeKey(key);
            if (isRowDataValue) {
                return zeroCopyGetGeneric(keyBytes, keyGroup);
            }
            byte[] valueBytes = NativeEngine.valueGetGeneric(stateHandle, keyBytes, keyGroup);
            if (valueBytes == null) {
                return getDefaultValue();
            }
            return deserializeValue(valueBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to access ValueState", e);
        }
    }

    /**
     * value() with RowData key — zero-serialization path.
     * Dispatches based on RowDataKeyAccessor strategy and value type.
     */
    @SuppressWarnings("unchecked")
    private V valueWithRowDataKey(K key, int keyGroup) throws IOException {
        RowDataKeyAccessor.Strategy keyStrategy = rowDataKeyAccessor.getStrategy();

        switch (keyStrategy) {
            case SINGLE_LONG: {
                long k = rowDataKeyAccessor.extractSingleLong(key);
                return valueForLongKey(k, keyGroup);
            }
            case SINGLE_INT: {
                int k = rowDataKeyAccessor.extractSingleInt(key);
                return valueForIntKey(k, keyGroup);
            }
            case FIXED_LENGTH_ROW: {
                long[] fields = rowDataKeyAccessor.extractFixedFields(key);
                return valueForFixedRowKey(fields, keyGroup);
            }
            default:
                // GENERIC / unsupported — fall through to serialized path
                byte[] keyBytes = serializeKey(key);
                if (isRowDataValue) {
                    return zeroCopyGetGeneric(keyBytes, keyGroup);
                }
                byte[] valueBytes = NativeEngine.valueGetGeneric(stateHandle, keyBytes, keyGroup);
                if (valueBytes == null) {
                    return getDefaultValue();
                }
                return deserializeValue(valueBytes);
        }
    }

    /** Get value for a long key extracted from RowData. */
    @SuppressWarnings("unchecked")
    private V valueForLongKey(long k, int keyGroup) throws IOException {
        if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
            if (!NativeEngine.valueContains(stateHandle, k, keyGroup)) {
                return getDefaultValue();
            }
            long raw = NativeEngine.valueGetLongLong(stateHandle, k, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(raw);
            }
            return (V) Long.valueOf(raw);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueContains(stateHandle, k, keyGroup)) {
                return getDefaultValue();
            }
            double raw = NativeEngine.valueGetLongDouble(stateHandle, k, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(Double.doubleToLongBits(raw));
            }
            return (V) Double.valueOf(raw);
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
            if (!NativeEngine.valueContainsInt(stateHandle, k, keyGroup)) {
                return getDefaultValue();
            }
            long raw = NativeEngine.valueGetIntLong(stateHandle, k, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(raw);
            }
            return (V) Long.valueOf(raw);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueContainsInt(stateHandle, k, keyGroup)) {
                return getDefaultValue();
            }
            double raw = NativeEngine.valueGetIntDouble(stateHandle, k, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(Double.doubleToLongBits(raw));
            }
            return (V) Double.valueOf(raw);
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
            if (!NativeEngine.valueContainsFixedRow(stateHandle, fields, keyGroup)) {
                return getDefaultValue();
            }
            long raw = NativeEngine.valueGetFixedRowLong(stateHandle, fields, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(raw);
            }
            return (V) Long.valueOf(raw);
        }
        if (valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
            if (!NativeEngine.valueContainsFixedRow(stateHandle, fields, keyGroup)) {
                return getDefaultValue();
            }
            double raw = NativeEngine.valueGetFixedRowDouble(stateHandle, fields, keyGroup);
            if (isRowDataValue) {
                return (V) rowDataValueAccessor.reconstructFromLong(Double.doubleToLongBits(raw));
            }
            return (V) Double.valueOf(raw);
        }
        // String/bytes value
        byte[] valueBytes = NativeEngine.valueGetFixedRowGeneric(stateHandle, fields, keyGroup);
        if (valueBytes == null) {
            return getDefaultValue();
        }
        return deserializeValue(valueBytes);
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
            // ========== RowData key paths (zero-serialization) ==========
            if (isRowDataKey && voidNamespace && rowDataKeyAccessor != null) {
                updateWithRowDataKey(key, keyGroup, value);
                return;
            }

            // ========== Primitive key fast paths ==========

            // Fast path: long key + long value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.valuePutLongLong(stateHandle, (Long) key, keyGroup, (Long) value);
                return;
            }
            // Fast path: long key + int value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT32) {
                NativeEngine.valuePutLongInt(stateHandle, (Long) key, keyGroup, (Integer) value);
                return;
            }
            // Fast path: long key + double value (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_FLOAT64) {
                NativeEngine.valuePutLongDouble(stateHandle, (Long) key, keyGroup, (Double) value);
                return;
            }
            // Fast path: long key + other value (String/BYTES/etc) (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                byte[] valueBytes = serializeValue(value);
                NativeEngine.valuePutLongString(stateHandle, (Long) key, keyGroup, valueBytes);
                return;
            }
            // Fast path: int key (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT32) {
                updateForIntKey((Integer) key, keyGroup, value);
                return;
            }
            // Generic path
            byte[] keyBytes = serializeKey(key);
            byte[] valueBytes = serializeValue(value);
            NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, valueBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update ValueState", e);
        }
    }

    /**
     * update() with RowData key — zero-serialization path.
     */
    private void updateWithRowDataKey(K key, int keyGroup, V value) throws IOException {
        RowDataKeyAccessor.Strategy keyStrategy = rowDataKeyAccessor.getStrategy();

        switch (keyStrategy) {
            case SINGLE_LONG: {
                long k = rowDataKeyAccessor.extractSingleLong(key);
                updateForLongKey(k, keyGroup, value);
                return;
            }
            case SINGLE_INT: {
                int k = rowDataKeyAccessor.extractSingleInt(key);
                updateForIntKey(k, keyGroup, value);
                return;
            }
            case FIXED_LENGTH_ROW: {
                long[] fields = rowDataKeyAccessor.extractFixedFields(key);
                updateForFixedRowKey(fields, keyGroup, value);
                return;
            }
            default:
                byte[] keyBytes = serializeKey(key);
                byte[] valueBytes = serializeValue(value);
                NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, valueBytes);
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
            // RowData key paths
            if (isRowDataKey && voidNamespace && rowDataKeyAccessor != null) {
                RowDataKeyAccessor.Strategy s = rowDataKeyAccessor.getStrategy();
                if (s == RowDataKeyAccessor.Strategy.SINGLE_LONG) {
                    long k = rowDataKeyAccessor.extractSingleLong(key);
                    NativeEngine.valueClearLong(stateHandle, k, keyGroup);
                    return;
                }
                if (s == RowDataKeyAccessor.Strategy.SINGLE_INT) {
                    int k = rowDataKeyAccessor.extractSingleInt(key);
                    NativeEngine.valueClearInt(stateHandle, k, keyGroup);
                    return;
                }
                if (s == RowDataKeyAccessor.Strategy.FIXED_LENGTH_ROW) {
                    long[] fields = rowDataKeyAccessor.extractFixedFields(key);
                    NativeEngine.valueClearFixedRow(stateHandle, fields, keyGroup);
                    return;
                }
            }

            // Primitive key paths
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.valueClearLong(stateHandle, (Long) key, keyGroup);
            } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT32) {
                NativeEngine.valueClearInt(stateHandle, (Integer) key, keyGroup);
            } else {
                byte[] keyBytes = serializeKey(key);
                NativeEngine.valueClearGeneric(stateHandle, keyBytes, keyGroup);
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

    /** Wrap native pointer [address, size] as BinaryRowData via off-heap MemorySegment. */
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
