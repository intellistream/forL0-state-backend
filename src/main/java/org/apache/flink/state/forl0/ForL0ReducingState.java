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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
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

    static final int BUILTIN_AGG_USER_DEFINED = -1;
    static final int BUILTIN_AGG_SUM = 0;
    static final int BUILTIN_AGG_MIN = 1;
    static final int BUILTIN_AGG_MAX = 2;

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
    private final boolean isTimeWindowNs;
    private final boolean isRowDataKey;
    private final boolean isStringKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataValue;
    private final RowDataKeyAccessor rowDataValueAccessor;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(128);
    private final long[] primitiveBuf = new long[1];
    private final long[] nativePtrBuf;
    private final int rowDataValueArity;
    private final int builtinAggType;

    // BYTES_TW batch buffer: accumulates TimeWindow pairs for a single key,
    // then flushes in one JNI call to amortize JNI crossing cost (25 windows → 1 crossing).
    private static final int BATCH_CAP = 64;  // max windows per batch (sliding window = 25)
    private final long[] batchNs = new long[BATCH_CAP * 2];  // interleaved [start, end, ...]
    private int batchCount = 0;
    private byte[] batchKeyBytes = null;
    private int batchKeyGroup = -1;
    private long batchValue = 0;

    // Key serialization cache: avoids re-serializing the same key object
    // across multiple windows (reference equality check — very fast).
    private Object lastSerKeyRef;
    private byte[] lastSerKeyBytes;

    private enum KeyNsStrategy { LONG_VOID, LONG_TW, BYTES_TW, GENERIC }
    private final KeyNsStrategy keyNsStrategy;

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
        this.isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isStringKey = keyTypeId == TypeAnalyzer.TYPE_STRING || keyTypeId == TypeAnalyzer.TYPE_BYTES;
        this.isRowDataValue = TypeAnalyzer.isRowDataSerializer(valueSerializer);
        this.rowDataValueAccessor = isRowDataValue
                ? TypeAnalyzer.createRowDataKeyAccessor(valueSerializer) : null;
        this.nativePtrBuf = isRowDataValue ? new long[2] : null;
        this.rowDataValueArity = isRowDataValue ? rowDataValueAccessor.getArity() : 0;
        this.builtinAggType = isRowDataValue
            ? BUILTIN_AGG_USER_DEFINED
            : resolveBuiltinAggType(reduceFunction, valueTypeId);

        if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_VOID;
        } else if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.LONG_TW;
        } else if (isTimeWindowNs && isStringKey && valueTypeId == TypeAnalyzer.TYPE_INT64) {
            this.keyNsStrategy = KeyNsStrategy.BYTES_TW;
        } else {
            this.keyNsStrategy = KeyNsStrategy.GENERIC;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get() {
        try {
            switch (keyNsStrategy) {
                case LONG_VOID: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, kg, primitiveBuf))
                            return getDefaultValue();
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) return zeroCopyGetLong(k, kg);
                    byte[] data = NativeEngine.valueGetLongString(stateHandle, k, kg);
                    return data != null ? deserializeValue(data) : getDefaultValue();
                }
                case LONG_TW: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.reduceGetLongWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd(), primitiveBuf))
                            return getDefaultValue();
                        if (isRowDataValue) {
                            return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
                        }
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) {
                        return zeroCopyGetLongWithTimeWindow(k, kg, tw.getStart(), tw.getEnd());
                    }
                    byte[] data = NativeEngine.valueGetLongStringWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd());
                    return data != null ? deserializeValue(data) : getDefaultValue();
                }
                case BYTES_TW: {
                    flushBatch();
                    byte[] keyBytes = serializeKeyOnlyCached(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    if (!NativeEngine.reduceGetBytesLongWithTW(stateHandle, keyBytes, kg,
                            tw.getStart(), tw.getEnd(), primitiveBuf)) {
                        return getDefaultValue();
                    }
                    return (V) Long.valueOf(primitiveBuf[0]);
                }
                default: {
                    byte[] keyBytes = serializeKey(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.valueGetGenericLongSafe(stateHandle, keyBytes, kg, primitiveBuf)) {
                            return getDefaultValue();
                        }
                        if (isRowDataValue) {
                            return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
                        }
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) return zeroCopyGetGeneric(keyBytes, kg);
                    byte[] data = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, kg);
                    return data != null ? deserializeValue(data) : getDefaultValue();
                }
            }
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
            switch (keyNsStrategy) {
                case LONG_VOID: {
                    long k = resolveKeyAsLong(key);
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (builtinAggType != BUILTIN_AGG_USER_DEFINED) {
                            NativeEngine.reduceAddLong(stateHandle, k, keyGroup, (Long) value, builtinAggType);
                            return;
                        }
                        if (NativeEngine.reduceGetAndPutLong(stateHandle, k, keyGroup, (Long) value, primitiveBuf)) {
                            @SuppressWarnings("unchecked")
                            V reduced = reduceFunction.reduce((V) Long.valueOf(primitiveBuf[0]), value);
                            NativeEngine.valuePutLongLong(stateHandle, k, keyGroup, (Long) reduced);
                        }
                    } else {
                        byte[] oldBytes = NativeEngine.valueGetLongString(stateHandle, k, keyGroup);
                        if (oldBytes == null) {
                            NativeEngine.valuePutLongString(stateHandle, k, keyGroup, serializeValue(value));
                        } else {
                            V oldValue = deserializeValue(oldBytes);
                            V reduced = reduceFunction.reduce(oldValue, value);
                            NativeEngine.valuePutLongString(stateHandle, k, keyGroup, serializeValue(reduced));
                        }
                    }
                    return;
                }
                case LONG_TW: {
                    long k = resolveKeyAsLong(key);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    long nsStart = tw.getStart(), nsEnd = tw.getEnd();
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (builtinAggType != BUILTIN_AGG_USER_DEFINED) {
                            NativeEngine.reduceAddLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, (Long) value, builtinAggType);
                            return;
                        }
                        if (NativeEngine.reduceGetAndPutLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, (Long) value, primitiveBuf)) {
                            @SuppressWarnings("unchecked")
                            V reduced = reduceFunction.reduce((V) Long.valueOf(primitiveBuf[0]), value);
                            NativeEngine.valuePutLongLongWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, (Long) reduced);
                        }
                    } else {
                        byte[] oldBytes = NativeEngine.valueGetLongStringWithTW(stateHandle, k, keyGroup, nsStart, nsEnd);
                        if (oldBytes == null) {
                            NativeEngine.valuePutLongStringWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, serializeValue(value));
                        } else {
                            V oldValue = deserializeValue(oldBytes);
                            V reduced = reduceFunction.reduce(oldValue, value);
                            NativeEngine.valuePutLongStringWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, serializeValue(reduced));
                        }
                    }
                    return;
                }
                case BYTES_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    long nsStart = tw.getStart(), nsEnd = tw.getEnd();
                    long longValue = (Long) value;

                    // Buffer the TimeWindow for batch flush (amortizes JNI crossing).
                    if (batchCount > 0 && (batchKeyGroup != keyGroup || batchValue != longValue)) {
                        flushBatch();
                    }
                    if (batchCount == 0) {
                        batchKeyBytes = serializeKeyOnlyCached(key);
                        batchKeyGroup = keyGroup;
                        batchValue = longValue;
                    }
                    batchNs[batchCount * 2] = nsStart;
                    batchNs[batchCount * 2 + 1] = nsEnd;
                    batchCount++;

                    if (builtinAggType != BUILTIN_AGG_USER_DEFINED) {
                        if (batchCount >= BATCH_CAP) {
                            flushBatch();
                        }
                    } else {
                        // User-defined reduce: flush immediately for read-modify-write.
                        flushBatch();
                        if (NativeEngine.reduceGetAndPutBytesLongWithTW(stateHandle, batchKeyBytes, keyGroup,
                                nsStart, nsEnd, longValue, primitiveBuf)) {
                            @SuppressWarnings("unchecked")
                            V reduced = reduceFunction.reduce((V) Long.valueOf(primitiveBuf[0]), value);
                            NativeEngine.valuePutBytesLongWithTW(stateHandle, batchKeyBytes, keyGroup,
                                    nsStart, nsEnd, (Long) reduced);
                        }
                    }
                    return;
                }
                default: {
                    byte[] keyBytes = serializeKey(key);
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        long longValue = (Long) value;
                        if (builtinAggType != BUILTIN_AGG_USER_DEFINED) {
                            NativeEngine.reduceAddGenericLong(stateHandle, keyBytes, keyGroup, longValue, builtinAggType);
                        } else if (NativeEngine.reduceGetAndPutGenericLong(stateHandle, keyBytes, keyGroup, longValue, primitiveBuf)) {
                            @SuppressWarnings("unchecked")
                            V reduced = reduceFunction.reduce((V) Long.valueOf(primitiveBuf[0]), value);
                            NativeEngine.valuePutGenericLong(stateHandle, keyBytes, keyGroup, (Long) reduced);
                        }
                        return;
                    }
                    byte[] oldBytes = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, keyGroup);
                    if (oldBytes == null) {
                        NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, serializeValue(value));
                    } else {
                        V oldValue = deserializeValue(oldBytes);
                        V newValue = reduceFunction.reduce(oldValue, value);
                        NativeEngine.valuePutGeneric(stateHandle, keyBytes, keyGroup, serializeValue(newValue));
                    }
                }
            }
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
            switch (keyNsStrategy) {
                case LONG_VOID: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.valueGetLongLongSafe(stateHandle, k, kg, primitiveBuf)) return null;
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) {
                        if (NativeEngine.valueGetLongStringPtr(stateHandle, k, kg, nativePtrBuf)) return wrapNativePtr();
                        return null;
                    }
                    byte[] data = NativeEngine.valueGetLongString(stateHandle, k, kg);
                    return data != null ? deserializeValue(data) : null;
                }
                case LONG_TW: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.reduceGetLongWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd(), primitiveBuf)) return null;
                        if (isRowDataValue) {
                            return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
                        }
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) {
                        if (NativeEngine.valueGetLongStringPtrWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd(), nativePtrBuf)) {
                            return wrapNativePtr();
                        }
                        return null;
                    }
                    byte[] data = NativeEngine.valueGetLongStringWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd());
                    return data != null ? deserializeValue(data) : null;
                }
                case BYTES_TW: {
                    flushBatch();
                    byte[] keyBytes = serializeKeyOnlyCached(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    if (!NativeEngine.reduceGetBytesLongWithTW(stateHandle, keyBytes, kg,
                            tw.getStart(), tw.getEnd(), primitiveBuf)) return null;
                    return (V) Long.valueOf(primitiveBuf[0]);
                }
                default: {
                    byte[] keyBytes = serializeKey(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        if (!NativeEngine.valueGetGenericLongSafe(stateHandle, keyBytes, kg, primitiveBuf)) return null;
                        if (isRowDataValue) {
                            return (V) rowDataValueAccessor.reconstructFromLong(primitiveBuf[0]);
                        }
                        return (V) Long.valueOf(primitiveBuf[0]);
                    }
                    if (isRowDataValue) {
                        if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, kg, nativePtrBuf)) return wrapNativePtr();
                        return null;
                    }
                    byte[] data = NativeEngine.reduceGetGeneric(stateHandle, keyBytes, kg);
                    return data != null ? deserializeValue(data) : null;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get internal ReducingState", e);
        }
    }

    @Override
    public void updateInternal(V valueToStore) {
        try {
            switch (keyNsStrategy) {
                case LONG_VOID: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        NativeEngine.valuePutLongLong(stateHandle, k, kg, (Long) valueToStore);
                    } else {
                        NativeEngine.valuePutLongString(stateHandle, k, kg, serializeValue(valueToStore));
                    }
                    return;
                }
                case LONG_TW: {
                    long k = resolveKeyAsLong(keyContext.currentKey);
                    int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        NativeEngine.valuePutLongLongWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd(), (Long) valueToStore);
                    } else {
                        NativeEngine.valuePutLongStringWithTW(stateHandle, k, kg, tw.getStart(), tw.getEnd(), serializeValue(valueToStore));
                    }
                    return;
                }
                case BYTES_TW: {
                    flushBatch();
                    byte[] keyBytes = serializeKeyOnlyCached(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.valuePutBytesLongWithTW(stateHandle, keyBytes,
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd(), (Long) valueToStore);
                    return;
                }
                default:
                    byte[] keyBytes = serializeKey(keyContext.currentKey);
                    if (valueTypeId == TypeAnalyzer.TYPE_INT64) {
                        NativeEngine.valuePutGenericLong(stateHandle, keyBytes,
                                keyContext.currentKeyGroupIndex, (Long) valueToStore);
                    } else {
                        NativeEngine.valuePutGeneric(stateHandle, keyBytes,
                                keyContext.currentKeyGroupIndex, serializeValue(valueToStore));
                    }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update internal ReducingState", e);
        }
    }

    @Override
    public void clear() {
        try {
            switch (keyNsStrategy) {
                case LONG_VOID:
                    NativeEngine.valueClearLong(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.valueClearWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return;
                }
                case BYTES_TW: {
                    flushBatch();
                    byte[] keyBytes = serializeKeyOnlyCached(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.valueClearBytesWithTW(stateHandle, keyBytes,
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return;
                }
                default:
                    byte[] keyBytes = serializeKey(keyContext.currentKey);
                    NativeEngine.valueClearGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
            }
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

    private V zeroCopyGetLong(long k, int keyGroup) {
        if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

    private V zeroCopyGetLongWithTimeWindow(long k, int keyGroup, long nsStart, long nsEnd) {
        if (NativeEngine.valueGetLongStringPtrWithTW(stateHandle, k, keyGroup, nsStart, nsEnd, nativePtrBuf)) {
            return wrapNativePtr();
        }
        return getDefaultValue();
    }

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

    private byte[] serializeKey(K key) throws IOException {
        keyOut.clear();
        if (!voidNamespace) {
            ((TypeSerializer<N>) namespaceSerializer).serialize(currentNamespace, keyOut);
        }
        keySerializer.serialize(key, keyOut);
        return keyOut.getCopyOfBuffer();
    }

    /**
     * Serialize key WITHOUT namespace prefix — used by BYTES_TW strategy where
     * the TimeWindow namespace is passed separately as (nsStart, nsEnd) longs.
     * Uses reference-equality cache: same key object → reuse previous bytes.
     * This saves 24 out of 25 serializations in sliding window scenarios.
     */
    private byte[] serializeKeyOnly(K key) throws IOException {
        keyOut.clear();
        keySerializer.serialize(key, keyOut);
        return keyOut.getCopyOfBuffer();
    }

    /** Cached variant: reuses bytes when the same key OBJECT reference is seen again. */
    private byte[] serializeKeyOnlyCached(K key) throws IOException {
        if (key == lastSerKeyRef && lastSerKeyBytes != null) {
            return lastSerKeyBytes;
        }
        lastSerKeyBytes = serializeKeyOnly(key);
        lastSerKeyRef = key;
        return lastSerKeyBytes;
    }

    /**
     * Flush buffered BYTES_TW batch to native engine in a single JNI call.
     * Called when: batch is full, key changes, or a get/clear operation needs fresh data.
     */
    private void flushBatch() {
        if (batchCount == 0) return;
        NativeEngine.reduceAddBytesLongWithTWBatch(
                stateHandle, batchKeyBytes, batchKeyGroup,
                batchNs, batchCount, batchValue, builtinAggType);
        batchCount = 0;
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

    static int resolveBuiltinAggType(ReduceFunction<?> function, int valueTypeId) {
        if (function == null || valueTypeId != TypeAnalyzer.TYPE_INT64) {
            return BUILTIN_AGG_USER_DEFINED;
        }
        SerializedLambda lambda = extractSerializedLambda(function);
        if (lambda == null || !"(JJ)J".equals(lambda.getImplMethodSignature())) {
            return BUILTIN_AGG_USER_DEFINED;
        }

        String implClass = lambda.getImplClass();
        String implMethod = lambda.getImplMethodName();
        if ("java/lang/Long".equals(implClass) && "sum".equals(implMethod)) {
            return BUILTIN_AGG_SUM;
        }
        if (("java/lang/Long".equals(implClass) || "java/lang/Math".equals(implClass))
                && "min".equals(implMethod)) {
            return BUILTIN_AGG_MIN;
        }
        if (("java/lang/Long".equals(implClass) || "java/lang/Math".equals(implClass))
                && "max".equals(implMethod)) {
            return BUILTIN_AGG_MAX;
        }
        return BUILTIN_AGG_USER_DEFINED;
    }

    private static SerializedLambda extractSerializedLambda(Object function) {
        try {
            Method writeReplace = function.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            Object replacement = writeReplace.invoke(function);
            return replacement instanceof SerializedLambda ? (SerializedLambda) replacement : null;
        } catch (ReflectiveOperationException | SecurityException ignored) {
            return null;
        }
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
