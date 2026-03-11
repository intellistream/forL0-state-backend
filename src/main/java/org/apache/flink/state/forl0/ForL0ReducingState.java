package org.apache.flink.state.forl0;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;

/**
 * ForL0 implementation of {@link InternalReducingState}.
 *
 * <p>JNI thin shell — state storage in C++ engine. For user-defined
 * {@link ReduceFunction}s, the reduce logic runs on Java side (reads
 * old accumulator from C++, applies reduce, writes back).
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of value
 */
public class ForL0ReducingState<K, N, V> implements InternalReducingState<K, N, V> {

    private final long stateHandle;
    private final ForL0KeyContext<K> keyContext;
    private final ReduceFunction<V> reduceFunction;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> valueSerializer;
    private V defaultValue;
    private N currentNamespace;

    private final int keyTypeId;
    private final int valueTypeId;
    private final boolean voidNamespace;
    private final boolean isRowDataKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataValue;
    private final RowDataKeyAccessor rowDataValueAccessor;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(128);
    private final long[] nativePtrBuf;
    private final int rowDataValueArity;

    ForL0ReducingState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            ReduceFunction<V> reduceFunction,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.stateHandle = stateHandle;
        this.keyContext = keyContext;
        this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        this.valueTypeId = TypeAnalyzer.getTypeId(valueSerializer);
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataValue = TypeAnalyzer.isRowDataSerializer(valueSerializer);
        this.rowDataValueAccessor = isRowDataValue
                ? TypeAnalyzer.createRowDataKeyAccessor(valueSerializer) : null;
        this.nativePtrBuf = isRowDataValue ? new long[2] : null;
        this.rowDataValueArity = isRowDataValue ? rowDataValueAccessor.getArity() : 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get() {
        try {
            // Long key + Long value fast path (VoidNamespace only)
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                if (!NativeEngine.valueContains(stateHandle, k, keyContext.currentKeyGroupIndex)) {
                    return getDefaultValue();
                }
                return (V) Long.valueOf(NativeEngine.valueGetLongLong(stateHandle, k, keyContext.currentKeyGroupIndex));
            }
            // Long key + other value: key not serialized
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                if (isRowDataValue) {
                    return zeroCopyGetLong(k, keyContext.currentKeyGroupIndex);
                }
                byte[] data = NativeEngine.valueGetLongString(stateHandle, k, keyContext.currentKeyGroupIndex);
                if (data == null) {
                    return getDefaultValue();
                }
                return deserializeValue(data);
            }
            // Generic path
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            if (isRowDataValue) {
                return zeroCopyGetGeneric(keyBytes, keyContext.currentKeyGroupIndex);
            }
            byte[] data = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
            if (data == null) {
                return getDefaultValue();
            }
            return deserializeValue(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get ReducingState", e);
        }
    }

    @Override
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to a ReducingState.");

        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;

        try {
            // Long key + Long value fast path
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(key);
                if (!NativeEngine.valueContains(stateHandle, k, keyGroup)) {
                    NativeEngine.valuePutLongLong(stateHandle, k, keyGroup, (Long) value);
                } else {
                    long oldValue = NativeEngine.valueGetLongLong(stateHandle, k, keyGroup);
                    @SuppressWarnings("unchecked")
                    V reduced = reduceFunction.reduce((V) Long.valueOf(oldValue), value);
                    NativeEngine.valuePutLongLong(stateHandle, k, keyGroup, (Long) reduced);
                }
                return;
            }
            // Long key + other value: key not serialized
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(key);
                V oldValue;
                if (isRowDataValue && NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, nativePtrBuf)) {
                    oldValue = wrapNativePtr();
                } else if (!isRowDataValue) {
                    byte[] oldBytes = NativeEngine.valueGetLongString(stateHandle, k, keyGroup);
                    oldValue = oldBytes != null ? deserializeValue(oldBytes) : null;
                } else {
                    oldValue = null;
                }
                V newValue = (oldValue == null) ? value : reduceFunction.reduce(oldValue, value);
                byte[] newBytes = serializeValue(newValue);
                NativeEngine.valuePutLongString(stateHandle, k, keyGroup, newBytes);
                return;
            }
            // Generic path: read-modify-write with serialized key
            byte[] keyBytes = serializeKey(key);
            V oldValue;
            if (isRowDataValue && NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyGroup, nativePtrBuf)) {
                oldValue = wrapNativePtr();
            } else if (!isRowDataValue) {
                byte[] oldBytes = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, keyGroup);
                oldValue = oldBytes != null ? deserializeValue(oldBytes) : null;
            } else {
                oldValue = null;
            }

            V newValue = (oldValue == null) ? value : reduceFunction.reduce(oldValue, value);
            byte[] newBytes = serializeValue(newValue);
            NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, newBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add to ReducingState", e);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        // Collect values from source namespaces and reduce
        V merged = null;
        N previousNamespace = currentNamespace;

        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            setCurrentNamespace(source);
            V sourceValue = getInternal();
            if (sourceValue != null) {
                clear();
                merged = (merged == null) ? sourceValue : reduceFunction.reduce(merged, sourceValue);
            }
        }

        if (merged != null) {
            setCurrentNamespace(target);
            V targetValue = getInternal();
            if (targetValue != null) {
                merged = reduceFunction.reduce(targetValue, merged);
            }
            updateInternal(merged);
        }
        setCurrentNamespace(previousNamespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V getInternal() {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                if (!NativeEngine.valueContains(stateHandle, k, keyContext.currentKeyGroupIndex)) {
                    return null;
                }
                return (V) Long.valueOf(NativeEngine.valueGetLongLong(stateHandle, k, keyContext.currentKeyGroupIndex));
            }
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                if (isRowDataValue) {
                    if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                        return wrapNativePtr();
                    }
                    return null;
                }
                byte[] data = NativeEngine.valueGetLongString(stateHandle, k, keyContext.currentKeyGroupIndex);
                if (data == null) return null;
                return deserializeValue(data);
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            if (isRowDataValue) {
                if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                    return wrapNativePtr();
                }
                return null;
            }
            byte[] data = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
            if (data == null) return null;
            return deserializeValue(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get internal ReducingState", e);
        }
    }

    @Override
    public void updateInternal(V valueToStore) {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                NativeEngine.valuePutLongLong(stateHandle, k, keyContext.currentKeyGroupIndex, (Long) valueToStore);
                return;
            }
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                byte[] valueBytes = serializeValue(valueToStore);
                NativeEngine.valuePutLongString(stateHandle, k, keyContext.currentKeyGroupIndex, valueBytes);
                return;
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            byte[] valueBytes = serializeValue(valueToStore);
            NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, valueBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update internal ReducingState", e);
        }
    }

    @Override
    public void clear() {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.valueClearLong(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                return;
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            NativeEngine.valueClearGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear ReducingState", e);
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

    // ========== Key resolution ==========

    /** Resolve key to long — works for both primitive Long and RowData[BIGINT]. */
    private long resolveKeyAsLong(K key) {
        if (isRowDataKey) {
            return rowDataKeyAccessor.extractSingleLong(key);
        }
        return (Long) key;
    }

    // ========== Zero-copy read helpers ==========

    @SuppressWarnings("unchecked")
    private V zeroCopyGetLong(long k, int keyGroup) {
        if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    @SuppressWarnings("unchecked")
    private V zeroCopyGetGeneric(byte[] keyBytes, int keyGroup) {
        if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyGroup, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    @SuppressWarnings("unchecked")
    private V wrapNativePtr() {
        int size = (int) nativePtrBuf[1];
        MemorySegment seg = MemorySegmentBridge.wrapNativeAddress(nativePtrBuf[0], size);
        BinaryRowData row = new BinaryRowData(rowDataValueArity);
        row.pointTo(seg, 0, size);
        return (V) row;
    }

    // ========== Serialization helpers ==========

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
    static <K, N, V, S extends State, IS extends S> IS create(
            StateDescriptor<S, V> stateDesc,
            long stateHandle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> stateSerializer,
            ForL0KeyedStateBackend<K> backend) {
        ReducingStateDescriptor<V> reducingDesc = (ReducingStateDescriptor<V>) stateDesc;
        return (IS) new ForL0ReducingState<>(
                stateHandle,
                backend.getForL0KeyContext(),
                reducingDesc.getReduceFunction(),
                backend.getKeySerializer(),
                namespaceSerializer,
                stateSerializer,
                stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, V, S extends State, IS extends S> IS update(
            StateDescriptor<S, V> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> stateSerializer,
            IS existingState) {
        return (IS) ((ForL0ReducingState<K, N, V>) existingState)
                .setNamespaceSerializer(namespaceSerializer)
                .setValueSerializer(stateSerializer)
                .setDefaultValue(stateDesc.getDefaultValue());
    }

    ForL0ReducingState<K, N, V> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0ReducingState<K, N, V> setValueSerializer(TypeSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0ReducingState<K, N, V> setDefaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
