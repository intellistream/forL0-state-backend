package org.apache.flink.state.forl0;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that uses Swiss Tables for state storage.
 *
 * <p>This is a lightweight StateBackend implementation with high-performance
 * hash table operations using SWAR parallel matching.
 *
 * @param <K> The key by which state is keyed.
 */
public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0KeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                    Tuple2.of(
                            StateDescriptor.Type.VALUE,
                            (StateCreateFactory) ForL0ValueState::create),
                    Tuple2.of(
                            StateDescriptor.Type.LIST,
                            (StateCreateFactory) ForL0ListState::create),
                    Tuple2.of(
                            StateDescriptor.Type.MAP,
                            (StateCreateFactory) ForL0MapState::create),
                    Tuple2.of(
                            StateDescriptor.Type.AGGREGATING,
                            (StateCreateFactory) ForL0AggregatingState::create),
                    Tuple2.of(
                            StateDescriptor.Type.REDUCING,
                            (StateCreateFactory) ForL0ReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                    Tuple2.of(
                            StateDescriptor.Type.VALUE,
                            (StateUpdateFactory) ForL0ValueState::update),
                    Tuple2.of(
                            StateDescriptor.Type.LIST,
                            (StateUpdateFactory) ForL0ListState::update),
                    Tuple2.of(
                            StateDescriptor.Type.MAP,
                            (StateUpdateFactory) ForL0MapState::update),
                    Tuple2.of(
                            StateDescriptor.Type.AGGREGATING,
                            (StateUpdateFactory) ForL0AggregatingState::update),
                    Tuple2.of(
                            StateDescriptor.Type.REDUCING,
                            (StateUpdateFactory) ForL0ReducingState::update))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** Map of created Key/Value states. */
    private final Map<String, State> createdKVStates;

    /** Map of registered Key/Value state stores. */
    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;

    /** The snapshot strategy for this backend. */
    private final ForL0SnapshotStrategy<K> snapshotStrategy;

    private final SnapshotExecutionType snapshotExecutionType;

    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueuesManager priorityQueuesManager;

    /** The ForL0 key context with public fields for direct access (hot path optimization). */
    private final ForL0KeyContext<K> forl0KeyContext;

    public ForL0KeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            ForL0SnapshotStrategy<K> snapshotStrategy,
            SnapshotExecutionType snapshotExecutionType,
            ForL0KeyContext<K> keyContext) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.forl0KeyContext = keyContext;
        this.registeredStores = registeredStores;
        this.createdKVStates = new HashMap<>();
        this.snapshotStrategy = snapshotStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());
        LOG.info("[ForL0] Initializing ForL0 keyed state backend with Swiss Tables.");
    }

    // ------------------------------------------------------------------------
    //  state backend operations
    // ------------------------------------------------------------------------

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        return priorityQueuesManager.createOrUpdate(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @SuppressWarnings({"unchecked"})
    private <N, V> ForL0StateStore<K, N, V> tryRegisterStateStore(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws StateMigrationException {

        ForL0StateStore<K, N, V> stateStore =
                (ForL0StateStore<K, N, V>) registeredStores.get(stateDesc.getName());

        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (stateStore != null) {
            RegisteredKeyValueStateBackendMetaInfo<N, V> restoredMetaInfo =
                    stateStore.getMetaInfo();

            restoredMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            TypeSerializer<N> previousNamespaceSerializer =
                    restoredMetaInfo.getNamespaceSerializer();

            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    restoredMetaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For ForL0 backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }

            restoredMetaInfo.checkStateMetaInfo(stateDesc);

            TypeSerializer<V> previousStateSerializer = restoredMetaInfo.getStateSerializer();

            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    restoredMetaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For ForL0 backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }

            RegisteredKeyValueStateBackendMetaInfo<N, V> updatedMetaInfo =
                    allowFutureMetadataUpdates
                            ? restoredMetaInfo.withSerializerUpgradesAllowed()
                            : restoredMetaInfo;

            stateStore.setMetaInfo(updatedMetaInfo);
        } else {
            RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            newStateSerializer,
                            snapshotTransformFactory);

            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;

            stateStore = createStateStore(keySerializer, newMetaInfo);
            registeredStores.put(stateDesc.getName(), stateStore);
        }

        return stateStore;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        if (!registeredStores.containsKey(state)) {
            return Stream.empty();
        }

        ForL0StateStore<K, N, ?> store =
                (ForL0StateStore<K, N, ?>) registeredStores.get(state);
        return store.getKeys(namespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(List<String> states, N namespace) {
        // Collect all keys from all stores, deduplicating across them
        return states.stream()
                .filter(registeredStores::containsKey)
                .flatMap(s -> {
                    ForL0StateStore<K, N, ?> store =
                            (ForL0StateStore<K, N, ?>) registeredStores.get(s);
                    return store.getKeys(namespace);
                })
                .distinct();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        if (!registeredStores.containsKey(state)) {
            return Stream.empty();
        }

        ForL0StateStore<K, N, ?> store =
                (ForL0StateStore<K, N, ?>) registeredStores.get(state);
        return store.getKeysAndNamespaces();
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        ForL0StateStore<K, N, SV> stateStore =
                tryRegisterStateStore(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory),
                        allowFutureMetadataUpdates);

        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(stateDesc, stateStore, this);
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(stateDesc, stateStore, createdState);
        }

        createdKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV> StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
            StateDescriptor<?, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        if (stateDesc instanceof ListStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else {
            return (StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
        }
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        SnapshotStrategyRunner<KeyedStateHandle, ?> snapshotStrategyRunner =
                new SnapshotStrategyRunner<>(
                        "ForL0 backend snapshot",
                        snapshotStrategy,
                        cancelStreamRegistry,
                        snapshotExecutionType);
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {
        ForL0SnapshotResources<K> snapshotResources =
                ForL0SnapshotResources.create(
                        registeredStores,
                        priorityQueuesManager.getRegisteredPQStates(),
                        keyGroupCompressionDecorator,
                        keyGroupRange,
                        keySerializer,
                        numberOfKeyGroups);

        return new SavepointResources<>(snapshotResources, snapshotExecutionType);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // Nothing to do
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {
            // Copy keys to list to avoid concurrency issues when state.clear() is called
            final List<K> keys = keyStream.collect(Collectors.toList());

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            for (K key : keys) {
                setCurrentKey(key);
                function.process(key, state);
            }
        }
    }

    @Override
    public String toString() {
        return "ForL0KeyedStateBackend";
    }

    /** Returns the total number of state entries across all keys/namespaces. */
    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int sum = 0;
        for (ForL0StateStore<K, ?, ?> store : registeredStores.values()) {
            sum += store.size();
        }
        return sum;
    }

    /** Returns the total number of state entries across all keys for the given namespace. */
    @VisibleForTesting
    public int numKeyValueStateEntries(Object namespace) {
        int sum = 0;
        for (ForL0StateStore<K, ?, ?> store : registeredStores.values()) {
            sum += store.sizeOfNamespace(namespace);
        }
        return sum;
    }

    /**
     * Get registered state stores for testing.
     */
    @VisibleForTesting
    public Map<String, ForL0StateStore<K, ?, ?>> getRegisteredStores() {
        return registeredStores;
    }

    /**
     * Returns the ForL0KeyContext for direct field access in State implementations.
     * This is used by State factory methods to pass the key context with public fields.
     */
    ForL0KeyContext<K> getForL0KeyContext() {
        return forl0KeyContext;
    }

    /**
     * Creates a StateStore with appropriate specialization based on key type.
     * 
     * <p>Specialization priority:
     * <ul>
     *   <li>Long keys → ForL0StateStoreLong (most common in Nexmark)</li>
     *   <li>Integer keys → ForL0StateStoreInt</li>
     *   <li>String keys → ForL0StateStoreString</li>
     *   <li>Other types → Generic ForL0StateStore</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private <N, V> ForL0StateStore<K, N, V> createStateStore(
            TypeSerializer<K> keySerializer,
            RegisteredKeyValueStateBackendMetaInfo<N, V> metaInfo) {
        
        // Detect key type from serializer class name
        String serializerName = keySerializer.getClass().getSimpleName();
        
        if (serializerName.contains("Long")) {
            // Long key specialization
            LOG.debug("[ForL0] Using specialized SwissTableLong for Long keys");
            return (ForL0StateStore<K, N, V>) new ForL0StateStoreLong<>(
                    keyContext.getKeyGroupRange(),
                    (TypeSerializer<Long>) keySerializer,
                    (RegisteredKeyValueStateBackendMetaInfo<N, V>) metaInfo);
        } else if (serializerName.contains("Int") && !serializerName.contains("Interval")) {
            // Integer key specialization
            LOG.debug("[ForL0] Using specialized SwissTableInt for Integer keys");
            return (ForL0StateStore<K, N, V>) new ForL0StateStoreInt<>(
                    keyContext.getKeyGroupRange(),
                    (TypeSerializer<Integer>) keySerializer,
                    (RegisteredKeyValueStateBackendMetaInfo<N, V>) metaInfo);
        } else {
            // Fallback to generic implementation (including String keys)
            LOG.debug("[ForL0] Using generic SwissTable for {} keys", serializerName);
            return new ForL0StateStore<>(
                    keyContext.getKeyGroupRange(),
                    keySerializer,
                    metaInfo);
        }
    }

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                ForL0StateStore<K, N, SV> stateStore,
                ForL0KeyedStateBackend<K> backend)
                throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc, ForL0StateStore<K, N, SV> stateStore, IS existingState)
                throws Exception;
    }
}
