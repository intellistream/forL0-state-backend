package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ForL0ListState<K, N, V> extends AbstractHeapMergingState<K, N, V, List<V>, Iterable<V>>
        implements InternalListState<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0ListState.class);

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    private ForL0ListState(
            StateTable<K, N, List<V>> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<List<V>> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            List<V> defaultValue) {
        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
        LOG.info("+++++++++++++++++ A forL0 list state is created ++++++++++++++++++++++");
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {return keySerializer;}

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {return valueSerializer;}

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {return namespaceSerializer;}

    // ------------------------------------------------------------------------
    //  state access
    // ------------------------------------------------------------------------

    @Override
    public Iterable<V> get() {
        return getInternal();
    }

    @Override
    public void add(V value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

        final N namespace = currentNamespace;

        final StateTable<K, N, List<V>> map = stateTable;
        List<V> list = map.get(namespace);

        if (list == null) {
            list = new ArrayList<>();
            map.put(namespace, list);
        }
        list.add(value);
        // 显式写回，确保底层 StateTable 能看到变更
        map.put(namespace, list);
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<List<V>> safeValueSerializer)
            throws IOException {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        List<V> result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

        if (result == null) {
            return null;
        }

        final TypeSerializer<V> dupSerializer =
                ((ListSerializer<V>) safeValueSerializer).getElementSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

        // write the same as RocksDB writes lists, with one ',' separator
        for (int i = 0; i < result.size(); i++) {
            dupSerializer.serialize(result.get(i), view);
            if (i < result.size() - 1) {
                view.writeByte(',');
            }
        }
        view.flush();

        return baos.toByteArray();
    }

    // ------------------------------------------------------------------------
    //  state merging
    // ------------------------------------------------------------------------

    @Override
    protected List<V> mergeState(List<V> a, List<V> b) {
        a.addAll(b);
        return a;
    }

    @Override
    public void update(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        if (values.isEmpty()) {
            clear();
            return;
        }

        List<V> newStateList = new ArrayList<>();
        for (V v : values) {
            Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
            newStateList.add(v);
        }

        stateTable.put(currentNamespace, newStateList);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        if (!values.isEmpty()) {
            stateTable.transform(
                    currentNamespace,
                    values,
                    (previousState, value) -> {
                        if (previousState == null) {
                            previousState = new ArrayList<>();
                        }
                        for (V v : value) {
                            Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
                            previousState.add(v);
                        }
                        return previousState;
                    });
        }
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new ForL0ListState<>(
                        (StateTable<K, N, List<E>>) stateTable,
                        keySerializer,
                        (TypeSerializer<List<E>>) stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        (List<E>) stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((ForL0ListState<K, N, E>) existingState)
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(
                                (TypeSerializer<List<E>>) stateTable.getStateSerializer())
                        .setDefaultValue((List<E>) stateDesc.getDefaultValue());
    }
}
