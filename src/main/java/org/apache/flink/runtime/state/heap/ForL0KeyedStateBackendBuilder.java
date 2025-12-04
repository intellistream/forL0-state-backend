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
    
    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;
    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;
    /** Memory manager for allocating memory for the state backend. */
    private final MemoryManager memoryManager;
    /** ForL0 StateBackend configuration. */
    private final ForL0StateBackendConfig forl0Config;

    /**
     * Main constructor with ForL0StateBackendConfig.
     * This is the recommended constructor for new code.
     */
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
            ForL0StateBackendConfig forl0Config) {
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
        this.forl0Config = forl0Config;
    }

    /**
     * Constructor with explicit l0CacheEnabled flag.
     * Kept for backward compatibility.
     */
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
                l0CacheEnabled 
                    ? new ForL0StateBackendConfig() 
                    : new ForL0StateBackendConfig().withL0CacheDisabled());
    }

    /**
     * Constructor with default configuration (L0 cache enabled).
     * Kept for backward compatibility.
     */
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
                new ForL0StateBackendConfig()); // default config with L0 enabled
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

        // Extract configuration values
        final boolean l0CacheEnabled = forl0Config.isL0CacheEnabled();
        final long l0MemoryMaxBytes = forl0Config.getL0MemoryMaxBytes();

        // Create shared L0Allocator for the entire backend (if L0 cache is enabled)
        @Nullable
        final L0MemoryAllocator sharedL0Allocator;
        if (l0CacheEnabled) {
            try {
                sharedL0Allocator = new NativeL0MemoryAllocator(l0MemoryMaxBytes);
                LOG.info("Created shared L0Allocator with capacity {} for ForL0KeyedStateBackend",
                        l0MemoryMaxBytes == -1 ? "unlimited" : l0MemoryMaxBytes + " bytes");
            } catch (Exception e) {
                throw new BackendBuildingException("Failed to create L0MemoryAllocator", e);
            }
        } else {
            sharedL0Allocator = null;
            LOG.info("L0 cache is disabled, ForL0KeyedStateBackend will not use L0 memory");
        }

        // Capture configuration for StateTableFactory
        final L0MemoryAllocator capturedL0Allocator = sharedL0Allocator;
        final ForL0StateBackendConfig capturedConfig = this.forl0Config;
        
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
                        capturedConfig, capturedL0Allocator);
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
