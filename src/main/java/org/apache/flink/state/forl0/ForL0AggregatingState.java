package org.apache.flink.state.forl0;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;

/**
 * ForL0 implementation of {@link InternalAggregatingState}.
 *
 * <p>JNI thin shell — accumulator stored in C++ engine. The
 * {@link AggregateFunction} runs on Java side (read accumulator from C++,
 * apply add/getResult, write back).
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <IN> The type of input values
 * @param <ACC> The type of accumulator
 * @param <OUT> The type of output values
 */
public class ForL0AggregatingState<K, N, IN, ACC, OUT> implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    private final long stateHandle;
    private final ForL0KeyContext<K> keyContext;
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<ACC> valueSerializer;
    private N currentNamespace;

    private final int keyTypeId;
    private final int valueTypeId;
    private final boolean voidNamespace;
    private final boolean isRowDataKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataAcc;
    private final RowDataKeyAccessor rowDataAccAccessor;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer accOut = new DataOutputSerializer(128);
    private final long[] nativePtrBuf;
    private final int rowDataAccArity;

    ForL0AggregatingState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> valueSerializer) {
        this.stateHandle = stateHandle;
        this.keyContext = keyContext;
        this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        this.valueTypeId = TypeAnalyzer.getTypeId(valueSerializer);
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataAcc = TypeAnalyzer.isRowDataSerializer(valueSerializer);
        this.rowDataAccAccessor = isRowDataAcc
                ? TypeAnalyzer.createRowDataKeyAccessor(valueSerializer) : null;
        this.nativePtrBuf = isRowDataAcc ? new long[2] : null;
        this.rowDataAccArity = isRowDataAcc ? rowDataAccAccessor.getArity() : 0;
    }

    @Override
    public OUT get() {
        try {
            // Long key fast path: avoid key serialization
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                ACC accumulator;
                if (isRowDataAcc && NativeEngine.valueGetLongStringPtr(stateHandle, k, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                    accumulator = wrapNativePtrAsAcc();
                } else if (!isRowDataAcc) {
                    byte[] data = NativeEngine.valueGetLongString(stateHandle, k, keyContext.currentKeyGroupIndex);
                    if (data == null) return null;
                    accumulator = deserializeAcc(data);
                } else {
                    return null;
                }
                return aggregateFunction.getResult(accumulator);
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            ACC accumulator;
            if (isRowDataAcc && NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                accumulator = wrapNativePtrAsAcc();
            } else if (!isRowDataAcc) {
                byte[] data = NativeEngine.aggGetGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
                if (data == null) return null;
                accumulator = deserializeAcc(data);
            } else {
                return null;
            }
            return aggregateFunction.getResult(accumulator);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get AggregatingState", e);
        }
    }

    @Override
    public void add(IN value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to an AggregatingState.");

        try {
            // Long key fast path: avoid key serialization
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                int keyGroup = keyContext.currentKeyGroupIndex;

                ACC accumulator;
                if (isRowDataAcc && NativeEngine.valueGetLongStringPtr(stateHandle, k, keyGroup, nativePtrBuf)) {
                    accumulator = wrapNativePtrAsAcc();
                } else if (!isRowDataAcc) {
                    byte[] oldBytes = NativeEngine.valueGetLongString(stateHandle, k, keyGroup);
                    accumulator = (oldBytes != null) ? deserializeAcc(oldBytes) : aggregateFunction.createAccumulator();
                } else {
                    accumulator = aggregateFunction.createAccumulator();
                }
                accumulator = aggregateFunction.add(value, accumulator);

                byte[] newBytes = serializeAcc(accumulator);
                NativeEngine.valuePutLongString(stateHandle, k, keyGroup, newBytes);
                return;
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            ACC accumulator;
            if (isRowDataAcc && NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                accumulator = wrapNativePtrAsAcc();
            } else if (!isRowDataAcc) {
                byte[] oldBytes = NativeEngine.aggGetGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
                accumulator = (oldBytes != null) ? deserializeAcc(oldBytes) : aggregateFunction.createAccumulator();
            } else {
                accumulator = aggregateFunction.createAccumulator();
            }

            accumulator = aggregateFunction.add(value, accumulator);

            byte[] newBytes = serializeAcc(accumulator);
            NativeEngine.aggAddGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, newBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add to AggregatingState", e);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        // Collect accumulators from source namespaces and merge
        ACC merged = null;
        N previousNamespace = currentNamespace;

        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            setCurrentNamespace(source);
            ACC sourceAcc = getInternal();
            if (sourceAcc != null) {
                clear();
                merged = (merged == null) ? sourceAcc : aggregateFunction.merge(merged, sourceAcc);
            }
        }

        if (merged != null) {
            setCurrentNamespace(target);
            ACC targetAcc = getInternal();
            if (targetAcc != null) {
                merged = aggregateFunction.merge(targetAcc, merged);
            }
            updateInternal(merged);
        }
        setCurrentNamespace(previousNamespace);
    }

    @Override
    public ACC getInternal() {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                if (isRowDataAcc) {
                    if (NativeEngine.valueGetLongStringPtr(stateHandle, k, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                        return wrapNativePtrAsAcc();
                    }
                    return null;
                }
                byte[] data = NativeEngine.valueGetLongString(stateHandle, k, keyContext.currentKeyGroupIndex);
                if (data == null) return null;
                return deserializeAcc(data);
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            if (isRowDataAcc) {
                if (NativeEngine.valueGetGenericPtr(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, nativePtrBuf)) {
                    return wrapNativePtrAsAcc();
                }
                return null;
            }
            byte[] data = NativeEngine.aggGetGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex);
            if (data == null) return null;
            return deserializeAcc(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get internal AggregatingState", e);
        }
    }

    @Override
    public void updateInternal(ACC valueToStore) {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                long k = resolveKeyAsLong(keyContext.currentKey);
                byte[] accBytes = serializeAcc(valueToStore);
                NativeEngine.valuePutLongString(stateHandle, k, keyContext.currentKeyGroupIndex, accBytes);
                return;
            }
            byte[] keyBytes = serializeKey(keyContext.currentKey);
            byte[] accBytes = serializeAcc(valueToStore);
            NativeEngine.aggAddGeneric(stateHandle, keyBytes, keyContext.currentKeyGroupIndex, accBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update internal AggregatingState", e);
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
            throw new RuntimeException("Failed to clear AggregatingState", e);
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
    public TypeSerializer<ACC> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<ACC> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, ACC> getStateIncrementalVisitor(
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

    // ========== Zero-copy read helper ==========

    @SuppressWarnings("unchecked")
    private ACC wrapNativePtrAsAcc() {
        int size = (int) nativePtrBuf[1];
        MemorySegment seg = MemorySegmentBridge.wrapNativeAddress(nativePtrBuf[0], size);
        BinaryRowData row = new BinaryRowData(rowDataAccArity);
        row.pointTo(seg, 0, size);
        return (ACC) row;
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

    private byte[] serializeAcc(ACC acc) throws IOException {
        if (isRowDataAcc) {
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytes(acc);
            if (raw != null) return raw;
        }
        accOut.clear();
        valueSerializer.serialize(acc, accOut);
        return accOut.getCopyOfBuffer();
    }

    @SuppressWarnings("unchecked")
    private ACC deserializeAcc(byte[] bytes) throws IOException {
        if (isRowDataAcc) {
            return (ACC) RowDataKeyAccessor.wrapBinaryRowData(bytes, rowDataAccAccessor.getArity());
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        return valueSerializer.deserialize(in);
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, IN, ACC, OUT, S extends State, IS extends S> IS create(
            StateDescriptor<S, ACC> stateDesc,
            long stateHandle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> stateSerializer,
            ForL0KeyedStateBackend<K> backend) {
        AggregatingStateDescriptor<IN, ACC, OUT> aggDesc = (AggregatingStateDescriptor<IN, ACC, OUT>) stateDesc;
        return (IS) new ForL0AggregatingState<>(
                stateHandle,
                backend.getForL0KeyContext(),
                aggDesc.getAggregateFunction(),
                backend.getKeySerializer(),
                namespaceSerializer,
                stateSerializer);
    }

    @SuppressWarnings("unchecked")
    static <K, N, IN, ACC, OUT, S extends State, IS extends S> IS update(
            StateDescriptor<S, ACC> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> stateSerializer,
            IS existingState) {
        return (IS) ((ForL0AggregatingState<K, N, IN, ACC, OUT>) existingState)
                .setNamespaceSerializer(namespaceSerializer)
                .setValueSerializer(stateSerializer);
    }

    ForL0AggregatingState<K, N, IN, ACC, OUT> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0AggregatingState<K, N, IN, ACC, OUT> setValueSerializer(TypeSerializer<ACC> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }
}
