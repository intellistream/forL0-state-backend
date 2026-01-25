package org.apache.flink.state.forl0;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * ForL0 implementation of {@link InternalAggregatingState}.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <IN> The type of input values
 * @param <ACC> The type of accumulator
 * @param <OUT> The type of output values
 */
public class ForL0AggregatingState<K, N, IN, ACC, OUT> implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    private final ForL0StateStore<K, N, ACC> store;
    private final ForL0KeyContext<K> keyContext;
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;
    private final AggregateTransformation<IN, ACC> aggregateTransformation;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<ACC> valueSerializer;
    private N currentNamespace;

    ForL0AggregatingState(
            ForL0StateStore<K, N, ACC> store,
            ForL0KeyContext<K> keyContext,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> valueSerializer) {
        this.store = store;
        this.keyContext = keyContext;
        this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
        this.aggregateTransformation = new AggregateTransformation<>(aggregateFunction);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public OUT get() {
        // Direct field access - no virtual method calls
        ACC accumulator = store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
        return accumulator != null ? aggregateFunction.getResult(accumulator) : null;
    }

    @Override
    public void add(IN value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to an AggregatingState.");
        
        // Direct field access - no virtual method calls
        store.transform(
                keyContext.currentKey,
                currentNamespace,
                value,
                aggregateTransformation,
                keyContext.currentKeyGroupIndex);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        
        // Direct field access - no virtual method calls
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        ACC merged = store.get(key, target, keyGroup);
        
        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            ACC sourceAcc = store.remove(key, source, keyGroup);
            if (sourceAcc != null) {
                if (merged == null) {
                    merged = sourceAcc;
                } else {
                    merged = aggregateFunction.merge(merged, sourceAcc);
                }
            }
        }
        
        if (merged != null) {
            store.put(key, target, merged, keyGroup);
        }
    }

    @Override
    public ACC getInternal() {
        // Direct field access - no virtual method calls
        return store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
    }

    @Override
    public void updateInternal(ACC valueToStore) {
        // Direct field access - no virtual method calls
        store.put(keyContext.currentKey, currentNamespace, valueToStore, keyContext.currentKeyGroupIndex);
    }

    @Override
    public void clear() {
        // Direct field access - no virtual method calls
        store.remove(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
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

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, IN, ACC, OUT, S extends State, IS extends S> IS create(
            StateDescriptor<S, ACC> stateDesc,
            ForL0StateStore<K, N, ACC> store,
            ForL0KeyedStateBackend<K> backend) {
        AggregatingStateDescriptor<IN, ACC, OUT> aggDesc = (AggregatingStateDescriptor<IN, ACC, OUT>) stateDesc;
        return (IS) new ForL0AggregatingState<>(
                store,
                backend.getForL0KeyContext(),
                aggDesc.getAggregateFunction(),
                backend.getKeySerializer(),
                store.getNamespaceSerializer(),
                store.getStateSerializer());
    }

    @SuppressWarnings("unchecked")
    static <K, N, IN, ACC, OUT, S extends State, IS extends S> IS update(
            StateDescriptor<S, ACC> stateDesc,
            ForL0StateStore<K, N, ACC> store,
            IS existingState) {
        return (IS) ((ForL0AggregatingState<K, N, IN, ACC, OUT>) existingState)
                .setNamespaceSerializer(store.getNamespaceSerializer())
                .setValueSerializer(store.getStateSerializer());
    }

    ForL0AggregatingState<K, N, IN, ACC, OUT> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0AggregatingState<K, N, IN, ACC, OUT> setValueSerializer(TypeSerializer<ACC> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    // ========== Inner class for aggregate transformation ==========

    /**
     * Implementation of {@link StateTransformationFunction} that wraps an {@link AggregateFunction}.
     */
    private static class AggregateTransformation<IN, ACC> implements StateTransformationFunction<ACC, IN> {
        private final AggregateFunction<IN, ACC, ?> aggregateFunction;

        AggregateTransformation(AggregateFunction<IN, ACC, ?> aggregateFunction) {
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public ACC apply(ACC previousState, IN value) throws Exception {
            ACC accumulator = previousState == null ? aggregateFunction.createAccumulator() : previousState;
            return aggregateFunction.add(value, accumulator);
        }
    }
}
