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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

/**
 * Builder class for {@link ForL0KeyedStateBackend} which handles all necessary
 * initializations and clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForL0KeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0KeyedStateBackendBuilder.class);

    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;

    public ForL0KeyedStateBackendBuilder(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            boolean asynchronousSnapshots,
            CloseableRegistry cancelStreamRegistry) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.asynchronousSnapshots = asynchronousSnapshots;
    }

    @Override
    public ForL0KeyedStateBackend<K> build() throws BackendBuildingException {
        LOG.info("[ForL0] Building ForL0KeyedStateBackend...");
        
        // Map of registered state stores
        Map<String, ForL0StateStore<K, ?, ?>> registeredStores = new HashMap<>();
        // Map of registered priority queue states
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates = new HashMap<>();
        
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        
        ForL0KeyContext<K> keyContext =
                new ForL0KeyContext<>(keyGroupRange, numberOfKeyGroups);

        // Initialize snapshot strategy
        ForL0SnapshotStrategy<K> snapshotStrategy = initSnapshotStrategy(
                registeredStores, registeredPQStates);

        // Restore state if any
        restoreState(registeredStores, registeredPQStates, keyContext);

        LOG.info("[ForL0] ForL0KeyedStateBackend built successfully.");
        
        return new ForL0KeyedStateBackend<>(
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistryForBackend,
                keyGroupCompressionDecorator,
                registeredStores,
                registeredPQStates,
                priorityQueueSetFactory,
                snapshotStrategy,
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS,
                keyContext);
    }

    private void restoreState(
            Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            InternalKeyContext<K> keyContext)
            throws BackendBuildingException {
        
        if (restoreStateHandles.isEmpty()) {
            LOG.info("[ForL0] No state to restore.");
            return;
        }

        LOG.info("[ForL0] Restoring state from {} handles.", restoreStateHandles.size());

        final RestoreOperation<Void> restoreOperation;
        final KeyedStateHandle firstHandle = restoreStateHandles.iterator().next();
        
        // Aligned with HeapKeyedStateBackendBuilder: use different restore operations
        // based on handle type for optimal performance
        if (firstHandle instanceof SavepointKeyedStateHandle) {
            LOG.info("[ForL0] Using savepoint restore operation.");
            restoreOperation = new ForL0SavepointRestoreOperation<>(
                    restoreStateHandles,
                    keySerializerProvider,
                    userCodeClassLoader,
                    registeredStores,
                    registeredPQStates,
                    priorityQueueSetFactory,
                    keyGroupRange,
                    numberOfKeyGroups);
        } else {
            LOG.info("[ForL0] Using checkpoint restore operation.");
            restoreOperation = new ForL0RestoreOperation<>(
                    restoreStateHandles,
                    keySerializerProvider,
                    userCodeClassLoader,
                    registeredStores,
                    registeredPQStates,
                    cancelStreamRegistry,
                    priorityQueueSetFactory,
                    keyGroupRange,
                    numberOfKeyGroups);
        }

        try {
            restoreOperation.restore();
            LOG.info("[ForL0] State restore completed.");
        } catch (Exception e) {
            throw new BackendBuildingException("Failed when trying to restore ForL0 backend", e);
        }
    }

    private ForL0SnapshotStrategy<K> initSnapshotStrategy(
            Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {
        return new ForL0SnapshotStrategy<>(
                registeredStores,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                keySerializerProvider.currentSchemaSerializer(),
                numberOfKeyGroups);
    }
}
