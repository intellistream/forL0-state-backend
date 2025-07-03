package org.apache.flink.runtime.state.heap;

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
import org.apache.flink.runtime.state.*;
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

public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0KeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                            Tuple2.of(StateDescriptor.Type.VALUE, (StateCreateFactory) ForL0ValueState::create),
                            Tuple2.of(StateDescriptor.Type.LIST, (StateCreateFactory) ForL0ListState::create),
                            Tuple2.of(StateDescriptor.Type.REDUCING, (StateCreateFactory) ForL0ReducingState::create),
                            Tuple2.of(StateDescriptor.Type.AGGREGATING, (StateCreateFactory) ForL0AggregatingState::create),
                            Tuple2.of(StateDescriptor.Type.MAP, (StateCreateFactory) ForL0MapState::create)
                        )
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                            Tuple2.of(StateDescriptor.Type.VALUE, (StateUpdateFactory) ForL0ValueState::update),
                            Tuple2.of(StateDescriptor.Type.LIST, (StateUpdateFactory) ForL0ListState::update),
                            Tuple2.of(StateDescriptor.Type.REDUCING, (StateUpdateFactory) ForL0ReducingState::update),
                            Tuple2.of(StateDescriptor.Type.AGGREGATING, (StateUpdateFactory) ForL0AggregatingState::update),
                            Tuple2.of(StateDescriptor.Type.MAP, (StateUpdateFactory) ForL0MapState::update)
                        )
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** Map of created Key/Value states. */
    private final Map<String, State> createdKVStates;

    /** Map of registered Key/Value states. */
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

    /** The configuration for local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;

    /** The snapshot strategy for this backend. */
    private final SnapshotStrategy<KeyedStateHandle, ?> checkpointStrategy;

    private final SnapshotExecutionType snapshotExecutionType;

    private final StateTableFactory<K> stateTableFactory;

    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueuesManager priorityQueuesManager;

    public ForL0KeyedStateBackend (
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            HeapSnapshotStrategy<K> checkpointStrategy, // TODO: need to be modified
            SnapshotExecutionType snapshotExecutionType,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
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
        this.registeredKVStates = registeredKVStates;
        this.createdKVStates = new HashMap<>();
        this.localRecoveryConfig = localRecoveryConfig;
        this.checkpointStrategy = checkpointStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.stateTableFactory = stateTableFactory;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());

        LOG.info("++++++++++++++++++ Initializing ForL0KeyedStateBackend ++++++++++++++++++++");
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
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    private <N, V> StateTable<K, N, V> tryRegisterStateTable(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<V> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws StateMigrationException {
        StateTable<K, N, V> stateTable =
                (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (stateTable != null) {
            RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo =
                    stateTable.getMetaInfo();

            restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<N> previousNamespaceSerializer =
                    restoredKvMetaInfo.getNamespaceSerializer();

            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }

            restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<V> previousStateSerializer = restoredKvMetaInfo.getStateSerializer();

            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }

            restoredKvMetaInfo =
                    allowFutureMetadataUpdates
                            ? restoredKvMetaInfo.withSerializerUpgradesAllowed()
                            : restoredKvMetaInfo;

            stateTable.setMetaInfo(restoredKvMetaInfo);
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

            stateTable = stateTableFactory.newStateTable(keyContext, newMetaInfo, keySerializer);
            registeredKVStates.put(stateDesc.getName(), stateTable);
        }

        return stateTable;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        // Nothing to do here
    }

    @Override
    public void notifyCheckpointAborted(long l) throws Exception {
        // Nothing to do here
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
                        "Heap backend snapshot",
                        checkpointStrategy,
                        cancelStreamRegistry,
                        snapshotExecutionType);
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {

        HeapSnapshotResources<K> snapshotResources =
                HeapSnapshotResources.create(
                        registeredKVStates,
                        priorityQueuesManager.getRegisteredPQStates(),
                        keyGroupCompressionDecorator,
                        keyGroupRange,
                        keySerializer,
                        numberOfKeyGroups);

        return new SavepointResources<>(snapshotResources, snapshotExecutionType);
    }

    @Override
    public String toString() { return "ForL0KeyedStateBackend"; }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int sum = 0;
        for (StateSnapshotRestore state : registeredKVStates.values()) {
            sum += ((StateTable<?, ?, ?>) state).size();
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeys(namespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeysAndNamespaces();
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        StateTable<K, N, SV> stateTable =
                tryRegisterStateTable(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory),
                        allowFutureMetadataUpdates);

        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState =
                    stateCreateFactory.createState(stateDesc, stateTable, getKeySerializer());
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(stateDesc, stateTable, createdState);
        }

        createdKVStates.put(stateDesc.getName(), createdState);
        return createdState;
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV> StateSnapshotTransformer.StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
            StateDescriptor<?, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        if (stateDesc instanceof ListStateDescriptor) {
            return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else {
            return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
        }
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
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

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<K> keys = keyStream.collect(Collectors.toList());

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            for (K key : keys) {
                setCurrentKey(key);
                function.process(key, state);
            }
        }
    }

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                StateTable<K, N, SV> stateTable,
                TypeSerializer<K> keySerializer)
            throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc,
                StateTable<K, N, SV> stateTable,
                IS existingState)
                throws Exception;
    }
}
