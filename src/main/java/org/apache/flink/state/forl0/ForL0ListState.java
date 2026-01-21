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
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ForL0 implementation of {@link InternalListState}.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of list elements
 */
public class ForL0ListState<K, N, V> implements InternalListState<K, N, V> {

    private final ForL0StateStore<K, N, List<V>> store;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<List<V>> valueSerializer;
    private List<V> defaultValue;
    private N currentNamespace;

    ForL0ListState(
            ForL0StateStore<K, N, List<V>> store,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<List<V>> valueSerializer,
            List<V> defaultValue) {
        this.store = store;
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
    }

    /** Empty state returned when no data exists. */
    private static final Iterable<?> EMPTY_STATE = java.util.Collections.emptyList();

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<V> get() {
        // Direct field access - no virtual method calls
        List<V> list = store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
        if (list != null) {
            return list;
        }
        return defaultValue != null ? valueSerializer.copy(defaultValue) : (Iterable<V>) EMPTY_STATE;
    }

    @Override
    public void add(V value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        
        // Direct field access - no virtual method calls
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        List<V> list = store.get(key, currentNamespace, keyGroup);
        
        if (list == null) {
            list = new ArrayList<>();
            store.put(key, currentNamespace, list, keyGroup);
        }
        list.add(value);
    }

    @Override
    public void update(List<V> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        
        if (values.isEmpty()) {
            clear();
            return;
        }
        
        List<V> newList = new ArrayList<>();
        for (V v : values) {
            Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
            newList.add(v);
        }
        // Direct field access - no virtual method calls
        store.put(keyContext.currentKey, currentNamespace, newList, keyContext.currentKeyGroupIndex);
    }

    @Override
    public void addAll(List<V> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        
        if (values.isEmpty()) {
            return;
        }
        
        // Direct field access - no virtual method calls
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        List<V> list = store.get(key, currentNamespace, keyGroup);
        
        if (list == null) {
            list = new ArrayList<>();
            store.put(key, currentNamespace, list, keyGroup);
        }
        for (V v : values) {
            Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
            list.add(v);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        
        // Direct field access - no virtual method calls
        K key = keyContext.currentKey;
        int keyGroup = keyContext.currentKeyGroupIndex;
        List<V> merged = store.get(key, target, keyGroup);
        
        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            List<V> sourceList = store.remove(key, source, keyGroup);
            if (sourceList != null) {
                if (merged == null) {
                    merged = sourceList;
                } else {
                    merged.addAll(sourceList);
                }
            }
        }
        
        if (merged != null) {
            store.put(key, target, merged, keyGroup);
        }
    }

    @Override
    public List<V> getInternal() {
        // Direct field access - no virtual method calls
        return store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
    }

    @Override
    public void updateInternal(List<V> valueToStore) {
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
    public TypeSerializer<List<V>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, V, S extends State, IS extends S> IS create(
            StateDescriptor<S, ?> stateDesc,
            ForL0StateStore<K, N, ?> store,
            ForL0KeyedStateBackend<K> backend) {
        ForL0StateStore<K, N, List<V>> listStore = (ForL0StateStore<K, N, List<V>>) store;
        return (IS) new ForL0ListState<>(
                listStore,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                listStore.getNamespaceSerializer(),
                listStore.getStateSerializer(),
                (List<V>) stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, V, S extends State, IS extends S> IS update(
            StateDescriptor<S, ?> stateDesc,
            ForL0StateStore<K, N, ?> store,
            IS existingState) {
        ForL0StateStore<K, N, List<V>> listStore = (ForL0StateStore<K, N, List<V>>) store;
        return (IS) ((ForL0ListState<K, N, V>) existingState)
                .setNamespaceSerializer(listStore.getNamespaceSerializer())
                .setValueSerializer(listStore.getStateSerializer())
                .setDefaultValue((List<V>) stateDesc.getDefaultValue());
    }

    ForL0ListState<K, N, V> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0ListState<K, N, V> setValueSerializer(TypeSerializer<List<V>> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0ListState<K, N, V> setDefaultValue(List<V> defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
