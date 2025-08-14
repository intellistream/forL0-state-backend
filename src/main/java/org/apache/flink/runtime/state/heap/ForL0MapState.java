package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class ForL0MapState<K, N, UK, UV> extends AbstractHeapState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0MapState.class);

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    private ForL0MapState(
            StateTable<K, N, Map<UK, UV>> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            Map<UK, UV> defaultValue) {
        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);

        Preconditions.checkState(
                valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        LOG.info("++++++++++++++++++ A forL0 map state is created ++++++++++++++++++++");
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public UV get(UK userKey) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);

        if (userMap == null) {
            return null;
        }

        return userMap.get(userKey);
    }

    @Override
    public void put(UK userKey, UV userValue) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        if (userMap == null) {
            userMap = new HashMap<>();
        }

        userMap.put(userKey, userValue);
        // 显式写回，确保持久化
        stateTable.put(currentNamespace, userMap);
    }

    @Override
    public void putAll(Map<UK, UV> value) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);

        if (userMap == null) {
            userMap = new HashMap<>();
        }

        userMap.putAll(value);
        // 显式写回
        stateTable.put(currentNamespace, userMap);
    }

    @Override
    public void remove(UK userKey) {

        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        if (userMap == null) {
            return;
        }

        userMap.remove(userKey);

        if (userMap.isEmpty()) {
            clear();
        } else {
            // 显式写回
            stateTable.put(currentNamespace, userMap);
        }
    }

    @Override
    public boolean contains(UK userKey) {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap != null && userMap.containsKey(userKey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.entrySet();
    }

    @Override
    public Iterable<UK> keys() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.keySet();
    }

    @Override
    public Iterable<UV> values() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptySet() : userMap.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null ? Collections.emptyIterator() : userMap.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() {
        Map<UK, UV> userMap = stateTable.get(currentNamespace);
        return userMap == null || userMap.isEmpty();
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        Map<UK, UV> result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

        if (result == null) {
            return null;
        }

        final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

        final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
        final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();

        return KvStateSerializer.serializeMap(
                result.entrySet(), dupUserKeySerializer, dupUserValueSerializer);
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer) {
        return (IS)
                new ForL0MapState<>(
                        (StateTable<K, N, Map<UK, UV>>) stateTable,
                        keySerializer,
                        (TypeSerializer<Map<UK, UV>>) stateTable.getStateSerializer(),
                        stateTable.getNamespaceSerializer(),
                        (Map<UK, UV>) stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState) {
        return (IS)
                ((ForL0MapState<K, N, UK, UV>) existingState)
                        .setNamespaceSerializer(stateTable.getNamespaceSerializer())
                        .setValueSerializer(
                                (TypeSerializer<Map<UK, UV>>) stateTable.getStateSerializer())
                        .setDefaultValue((Map<UK, UV>) stateDesc.getDefaultValue());
    }
}
