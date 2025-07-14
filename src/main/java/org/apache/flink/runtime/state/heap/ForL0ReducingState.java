package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ForL0ReducingState<K, N, V> extends AbstractHeapMergingState<K, N, V, V, V>
    implements InternalReducingState<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0ReducingState.class);

    private ReduceTransformation<V> reduceTransformation;

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     * @param reduceFunction The reduce function used for reducing state.
     */
    private ForL0ReducingState(
            StateTable<K, N, V> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            V defaultValue,
            ReduceFunction<V> reduceFunction) {

        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
        this.reduceTransformation = new ReduceTransformation<>(reduceFunction);
        LOG.info("++++++++++++++++++ A forL0 reducing state is created ++++++++++++++++++++");
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

    // ------------------------------------------------------------------------
    //  state access
    // ------------------------------------------------------------------------

    @Override
    public V get() {
        return getInternal();
    }

    @Override
    public void add(V value) throws Exception {

        if (value == null) {
            clear();
            return;
        }

        stateTable.transform(currentNamespace, value, reduceTransformation);
    }

    // ------------------------------------------------------------------------
    //  state merging
    // ------------------------------------------------------------------------

    @Override
    protected V mergeState(V a, V b) throws Exception {
        return reduceTransformation.apply(a, b);
    }

    ForL0ReducingState<K, N, V> setReducingFunction(ReduceFunction<V> reduceFunction) {
        this.reduceTransformation = new ReduceTransformation<>(reduceFunction);
        return this;
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new ForL0ReducingState<>(
                        stateTable,
                        keySerializer,
                        stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        stateDesc.getDefaultValue(),
                        ((ReducingStateDescriptor<SV>) stateDesc).getReduceFunction());
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((ForL0ReducingState<K, N, SV>) existingState)
                        .setReducingFunction(
                                ((ReducingStateDescriptor<SV>) stateDesc).getReduceFunction())
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(stateTable.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }

    static final class ReduceTransformation<V> implements StateTransformationFunction<V, V> {
        private final ReduceFunction<V> reduceFunction;
        ReduceTransformation(ReduceFunction<V> reduceFunction) {
            this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
        }

        @Override
        public V apply(V previousState, V value) throws Exception {
            return previousState != null ? reduceFunction.reduce(previousState, value) : value;
        }
    }

}
