package org.apache.flink.runtime.state.heap;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ForL0AggregatingState<K, N, IN, ACC, OUT> extends AbstractHeapMergingState<K, N, IN, ACC, OUT>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0AggregatingState.class);

    private AggregateTransformation<IN, ACC, OUT> aggregateTransformation;

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     * @param aggregateFunction The aggregating function used for aggregating state.
     */
    private ForL0AggregatingState(
            StateTable<K, N, ACC> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<ACC> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            ACC defaultValue,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {

        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
        this.aggregateTransformation = new AggregateTransformation<>(aggregateFunction);
        LOG.info("++++++++++++++++++ A forL0 aggregating state is created ++++++++++++++++++++");
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

    // ------------------------------------------------------------------------
    //  state access
    // ------------------------------------------------------------------------

    @Override
    public OUT get() {
        ACC accumulator = getInternal();
        return accumulator != null
                ? aggregateTransformation.aggFunction.getResult(accumulator)
                : null;
    }

    @Override
    public void add(IN value) throws Exception {
        final N namespace = currentNamespace;

        if (value == null) {
            clear();
            return;
        }
        stateTable.transform(namespace, value, aggregateTransformation);
    }

    // ------------------------------------------------------------------------
    //  state merging
    // ------------------------------------------------------------------------

    @Override
    protected ACC mergeState(ACC a, ACC b) {
        return aggregateTransformation.aggFunction.merge(a, b);
    }

    ForL0AggregatingState<K, N, IN, ACC, OUT> setAggregateFunction(
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        this.aggregateTransformation = new AggregateTransformation<>(aggregateFunction);
        return this;
    }

    @SuppressWarnings("unchecked")
    static <T, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new ForL0AggregatingState<>(
                        stateTable,
                        keySerializer,
                        stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        stateDesc.getDefaultValue(),
                        ((AggregatingStateDescriptor<T, SV, ?>) stateDesc).getAggregateFunction());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((ForL0AggregatingState<K, N, T, SV, ?>) existingState)
                        .setAggregateFunction(
                                ((AggregatingStateDescriptor) stateDesc).getAggregateFunction())
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(stateTable.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }

    static final class AggregateTransformation<IN, ACC, OUT>
            implements StateTransformationFunction<ACC, IN> {
        private final AggregateFunction<IN, ACC, OUT> aggFunction;
        public AggregateTransformation(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
            this.aggFunction = Preconditions.checkNotNull(aggregateFunction);
        }

        @Override
        public ACC apply(ACC acc, IN value) {
            if (acc == null) {
                acc = aggFunction.createAccumulator();
            }
            return aggFunction.add(value, acc);
        }
    }
}
