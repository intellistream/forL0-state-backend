package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;
import org.apache.flink.runtime.state.heap.space.NativeL0MemoryAllocator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

public class ForL0KeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ForL0KeyedStateBackendBuilder.class);
    
    /** Default L0 memory capacity in bytes (64 MB). */
    private static final long DEFAULT_L0_MEMORY_CAPACITY = 64 * 1024 * 1024;
    
    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;
    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;
    /** Memory manager for allocating memory for the state backend. */
    private final MemoryManager memoryManager;
    /** Whether L0 cache is enabled for ForL0StateMap. */
    private final boolean l0CacheEnabled;

    // 主构造器：显式指定是否启用L0缓存
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
            MemoryManager memoryManager,
            boolean l0CacheEnabled) {
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
        this.l0CacheEnabled = l0CacheEnabled;
    }

    // 兼容旧签名：默认不启用L0缓存
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
        this(
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
                localRecoveryConfig,
                priorityQueueSetFactory,
                asynchronousSnapshots,
                cancelStreamRegistry,
                memoryManager,
                true); // For now the L0 cache is enabled here
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

        // Create shared L0Allocator for the entire backend (if L0 cache is enabled)
        @Nullable
        final L0MemoryAllocator sharedL0Allocator;
        if (l0CacheEnabled) {
            try {
                sharedL0Allocator = new NativeL0MemoryAllocator(DEFAULT_L0_MEMORY_CAPACITY);
                LOG.info("Created shared L0Allocator with capacity {} bytes for ForL0KeyedStateBackend",
                        DEFAULT_L0_MEMORY_CAPACITY);
            } catch (Exception e) {
                throw new BackendBuildingException("Failed to create L0MemoryAllocator", e);
            }
        } else {
            sharedL0Allocator = null;
            LOG.info("L0 cache is disabled, ForL0KeyedStateBackend will not use L0 memory");
        }

        // Capture the shared L0Allocator so that each StateTable uses the same allocator
        final L0MemoryAllocator capturedL0Allocator = sharedL0Allocator;
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
                return ForL0StateTable.create(keyContext, keyValueStateMetaInfo, keySerializer, mm, 
                        l0CacheEnabled, capturedL0Allocator);
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
                memoryManager,
                sharedL0Allocator);
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
