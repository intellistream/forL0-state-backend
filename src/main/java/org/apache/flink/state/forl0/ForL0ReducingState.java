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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * ForL0 implementation of {@link InternalReducingState}.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of value
 */
public class ForL0ReducingState<K, N, V> implements InternalReducingState<K, N, V> {

    private final ForL0StateStore<K, N, V> store;
    private final ForL0KeyContext<K> keyContext;
    private final ReduceFunction<V> reduceFunction;
    private ReduceTransformation<V> reduceTransformation;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> valueSerializer;
    private V defaultValue;
    private N currentNamespace;

    ForL0ReducingState(
            ForL0StateStore<K, N, V> store,
            ForL0KeyContext<K> keyContext,
            ReduceFunction<V> reduceFunction,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.store = store;
        this.keyContext = keyContext;
        this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
        this.reduceTransformation = new ReduceTransformation<>(reduceFunction);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
    }

    @Override
    public V get() {
        // Direct field access - no virtual method calls
        V val = store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
        return val != null ? val : getDefaultValue();
    }

    @Override
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to a ReducingState.");
        
        // Direct field access - no virtual method calls
        store.transform(
                keyContext.currentKey,
                currentNamespace,
                value,
                reduceTransformation,
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
        V merged = store.get(key, target, keyGroup);
        
        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            V sourceValue = store.remove(key, source, keyGroup);
            if (sourceValue != null) {
                if (merged == null) {
                    merged = sourceValue;
                } else {
                    merged = reduceFunction.reduce(merged, sourceValue);
                }
            }
        }
        
        if (merged != null) {
            store.put(key, target, merged, keyGroup);
        }
    }

    @Override
    public V getInternal() {
        // Direct field access - no virtual method calls
        return store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
    }

    @Override
    public void updateInternal(V valueToStore) {
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

    private V getDefaultValue() {
        return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, V, S extends State, IS extends S> IS create(
            StateDescriptor<S, V> stateDesc,
            ForL0StateStore<K, N, V> store,
            ForL0KeyedStateBackend<K> backend) {
        ReducingStateDescriptor<V> reducingDesc = (ReducingStateDescriptor<V>) stateDesc;
        return (IS) new ForL0ReducingState<>(
                store,
                backend.getForL0KeyContext(),
                reducingDesc.getReduceFunction(),
                backend.getKeySerializer(),
                store.getNamespaceSerializer(),
                store.getStateSerializer(),
                stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, V, S extends State, IS extends S> IS update(
            StateDescriptor<S, V> stateDesc,
            ForL0StateStore<K, N, V> store,
            IS existingState) {
        return (IS) ((ForL0ReducingState<K, N, V>) existingState)
                .setNamespaceSerializer(store.getNamespaceSerializer())
                .setValueSerializer(store.getStateSerializer())
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

    // ========== Inner class for reduce transformation ==========

    /**
     * Implementation of {@link StateTransformationFunction} that wraps a {@link ReduceFunction}.
     */
    private static class ReduceTransformation<V> implements StateTransformationFunction<V, V> {
        private final ReduceFunction<V> reduceFunction;

        ReduceTransformation(ReduceFunction<V> reduceFunction) {
            this.reduceFunction = reduceFunction;
        }

        @Override
        public V apply(V previousState, V value) throws Exception {
            return previousState == null ? value : reduceFunction.reduce(previousState, value);
        }
    }
}
