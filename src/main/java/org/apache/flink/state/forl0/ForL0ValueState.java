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
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * ForL0 implementation of {@link InternalValueState}.
 * 
 * <p>Simple and direct implementation without unnecessary abstraction.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of value
 */
public class ForL0ValueState<K, N, V> implements InternalValueState<K, N, V> {

    private final ForL0StateStore<K, N, V> store;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> valueSerializer;
    private V defaultValue;
    private N currentNamespace;

    ForL0ValueState(
            ForL0StateStore<K, N, V> store,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        this.store = store;
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
    }

    @Override
    public V value() {
        // Direct field access - no virtual method calls
        V val = store.get(keyContext.currentKey, currentNamespace, keyContext.currentKeyGroupIndex);
        return val != null ? val : getDefaultValue();
    }

    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }
        // Direct field access - no virtual method calls
        store.put(keyContext.currentKey, currentNamespace, value, keyContext.currentKeyGroupIndex);
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

        // Queryable state requires key context which we don't have here
        // For now, throw UnsupportedOperationException - queryable state not supported
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    private V getDefaultValue() {
        return defaultValue != null ? valueSerializer.copy(defaultValue) : null;
    }

    // ========== Factory Methods ==========

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            ForL0StateStore<K, N, SV> store,
            ForL0KeyedStateBackend<K> backend) {
        return (IS) new ForL0ValueState<>(
                store,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                store.getNamespaceSerializer(),
                store.getStateSerializer(),
                stateDesc.getDefaultValue());
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            ForL0StateStore<K, N, SV> store,
            IS existingState) {
        return (IS) ((ForL0ValueState<K, N, SV>) existingState)
                .setNamespaceSerializer(store.getNamespaceSerializer())
                .setValueSerializer(store.getStateSerializer())
                .setDefaultValue(stateDesc.getDefaultValue());
    }

    ForL0ValueState<K, N, V> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0ValueState<K, N, V> setValueSerializer(TypeSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0ValueState<K, N, V> setDefaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
