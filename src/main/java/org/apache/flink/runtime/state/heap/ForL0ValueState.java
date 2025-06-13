package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

class ForL0ValueState<K, N, V> extends AbstractForL0State<K, N, V>
        implements InternalValueState<K, N, V> {

    private ForL0ValueState(
            StateTable<K, N, V> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            V defaultValue) {
        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
    }

    @Override
    public V value() {
        final V result = stateTable.get(currentNamespace);
        if (result == null) {
            return getDefaultValue();
        }
        return result;
    }

    @Override
    public void update(V v) {
        if (v == null) {
            clear();
            return;
        }
        stateTable.put(currentNamespace, v);
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

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new ForL0ValueState<>(
                        stateTable,
                        keySerializer,
                        stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((ForL0ValueState<K, N, SV>) existingState)
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(stateTable.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }
}
