/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * ForL0 implementation of {@link InternalMapState}.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <UK> The type of user map key
 * @param <UV> The type of user map value
 */
public class ForL0MapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {

    private final ForL0StateStore<K, N, Map<UK, UV>> store;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<Map<UK, UV>> valueSerializer;
    private N currentNamespace;

    ForL0MapState(
            ForL0StateStore<K, N, Map<UK, UV>> store,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer) {
        this.store = store;
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    private Map<UK, UV> getOrCreateMap() {
        // Direct field access - no virtual method calls
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        Map<UK, UV> map = store.get(key, currentNamespace, keyGroup);
        if (map == null) {
            map = new HashMap<>();
            store.put(key, currentNamespace, map, keyGroup);
        }
        return map;
    }

    private Map<UK, UV> getMapOrNull() {
        // Direct field access - no virtual method calls
        return store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
    }

    @Override
    public UV get(UK userKey) {
        Map<UK, UV> map = getMapOrNull();
        return map != null ? map.get(userKey) : null;
    }

    @Override
    public void put(UK userKey, UV userValue) {
        Map<UK, UV> map = getOrCreateMap();
        map.put(userKey, userValue);
    }

    @Override
    public void putAll(Map<UK, UV> map) {
        if (map == null || map.isEmpty()) {
            return;
        }
        Map<UK, UV> stateMap = getOrCreateMap();
        stateMap.putAll(map);
    }

    @Override
    public void remove(UK userKey) {
        Map<UK, UV> map = getMapOrNull();
        if (map == null) {
            return;
        }
        
        map.remove(userKey);
        
        if (map.isEmpty()) {
            clear();
        }
    }

    @Override
    public boolean contains(UK userKey) {
        Map<UK, UV> map = getMapOrNull();
        return map != null && map.containsKey(userKey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        Map<UK, UV> map = getMapOrNull();
        return map != null ? map.entrySet() : java.util.Collections.emptySet();
    }

    @Override
    public Iterable<UK> keys() {
        Map<UK, UV> map = getMapOrNull();
        return map != null ? map.keySet() : java.util.Collections.emptySet();
    }

    @Override
    public Iterable<UV> values() {
        Map<UK, UV> map = getMapOrNull();
        return map != null ? map.values() : java.util.Collections.emptyList();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        Map<UK, UV> map = getMapOrNull();
        return map != null ? map.entrySet().iterator() : java.util.Collections.emptyIterator();
    }

    @Override
    public boolean isEmpty() {
        Map<UK, UV> map = getMapOrNull();
        return map == null || map.isEmpty();
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, UK, UV, S extends State, IS extends S> IS create(
            StateDescriptor<S, ?> stateDesc,
            ForL0StateStore<K, N, ?> store,
            ForL0KeyedStateBackend<K> backend) {
        ForL0StateStore<K, N, Map<UK, UV>> mapStore = (ForL0StateStore<K, N, Map<UK, UV>>) store;
        return (IS) new ForL0MapState<>(
                mapStore,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                mapStore.getNamespaceSerializer(),
                mapStore.getStateSerializer());
    }

    @SuppressWarnings("unchecked")
    static <K, N, UK, UV, S extends State, IS extends S> IS update(
            StateDescriptor<S, ?> stateDesc,
            ForL0StateStore<K, N, ?> store,
            IS existingState) {
        ForL0StateStore<K, N, Map<UK, UV>> mapStore = (ForL0StateStore<K, N, Map<UK, UV>>) store;
        return (IS) ((ForL0MapState<K, N, UK, UV>) existingState)
                .setNamespaceSerializer(mapStore.getNamespaceSerializer())
                .setValueSerializer(mapStore.getStateSerializer());
    }

    ForL0MapState<K, N, UK, UV> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0MapState<K, N, UK, UV> setValueSerializer(TypeSerializer<Map<UK, UV>> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }
}
