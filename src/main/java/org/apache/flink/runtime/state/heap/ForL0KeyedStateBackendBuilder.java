package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

public class ForL0KeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;
    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;
    /** Memory manager for allocating memory for the state backend. */
    private final MemoryManager memoryManager;

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
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            boolean asynchronousSnapshots,
            CloseableRegistry cancelStreamRegistry,
            MemoryManager memoryManager) {
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
        this.localRecoveryConfig = localRecoveryConfig;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.asynchronousSnapshots = asynchronousSnapshots;
        this.memoryManager = memoryManager;
    }

    @Override
    public ForL0KeyedStateBackend<K> build() throws BackendBuildingException {
        Map<String, StateTable<K, ?, ?>> registeredKVStates = new HashMap<>();
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates = new HashMap<>();
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();

        HeapSnapshotStrategy<K> snapshotStrategy =
                initSnapshotStrategy(registeredKVStates, registeredPQStates);
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);

        // capture the memoryManager so that each StateTable gets its own allocator
        final StateTableFactory<K> stateTableFactory = new StateTableFactory<K>() {
            @Override
            public <N, V> StateTable<K, N, V> newStateTable(InternalKeyContext<K> keyContext,
                                                            RegisteredKeyValueStateBackendMetaInfo<N, V> keyValueStateMetaInfo,
                                                            TypeSerializer<K> keySerializer) {
                // Use the static factory method to ensure MemoryManager is available during construction
                MemoryManager mm = ForL0KeyedStateBackendBuilder.this.memoryManager;
                if (mm == null) {
                    throw new IllegalStateException("MemoryManager is null in ForL0KeyedStateBackendBuilder. " +
                        "This indicates that the MemoryManager was not properly passed from the Environment.");
                }
                return ForL0StateTable.create(keyContext, keyValueStateMetaInfo, keySerializer, mm);
            }
        };

        restoreState(registeredKVStates, registeredPQStates, keyContext, stateTableFactory);
        return new ForL0KeyedStateBackend<>(
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistryForBackend,
                keyGroupCompressionDecorator,
                registeredKVStates,
                registeredPQStates,
                localRecoveryConfig,
                priorityQueueSetFactory,
                snapshotStrategy,
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS,
                stateTableFactory,
                keyContext,
                memoryManager);
    }

    // Below methods are copied from heap state, may need to be modified

    private void restoreState(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            InternalKeyContext<K> keyContext,
            StateTableFactory<K> stateTableFactory)
            throws BackendBuildingException {
        final RestoreOperation<Void> restoreOperation;

        final KeyedStateHandle firstHandle;
        if (restoreStateHandles.isEmpty()) {
            firstHandle = null;
        } else {
            firstHandle = restoreStateHandles.iterator().next();
        }
        if (firstHandle instanceof SavepointKeyedStateHandle) {
            restoreOperation =
                    new HeapSavepointRestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext);
        } else {
            // Use ForL0RestoreOperation instead of HeapRestoreOperation
            restoreOperation =
                    new ForL0RestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            cancelStreamRegistry,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext);
        }
        try {
            restoreOperation.restore();
            logger.info("Finished to build ForL0 keyed state-backend.");
        } catch (Exception e) {
            throw new BackendBuildingException("Failed when trying to restore ForL0 backend", e);
        }
    }

    private HeapSnapshotStrategy<K> initSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {
        return new HeapSnapshotStrategy<>(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                localRecoveryConfig,
                keyGroupRange,
                keySerializerProvider,
                numberOfKeyGroups);
    }
}
