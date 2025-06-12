package org.apache.flink.runtime.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.*;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ForL0KeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0KeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                        Tuple2.of(StateDescriptor.Type.VALUE, (StateCreateFactory) ForL0ValueState::create)
                        // more types to be added
                        )
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                        Tuple2.of(StateDescriptor.Type.VALUE, (StateUpdateFactory) ForL0ValueState::update)
                        // more types to be added
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
            HeapSnapshotStrategy<K> checkpointStrategy,
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

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {

    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        return null;
    }

    @Override
    public int numKeyValueStateEntries() {
        return 0;
    }

    @Override
    public <N> Stream<K> getKeys(String s, N n) {
        return Stream.empty();
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String s) {
        return Stream.empty();
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(@Nonnull TypeSerializer<N> typeSerializer, @Nonnull StateDescriptor<S, SV> stateDescriptor, @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> stateSnapshotTransformFactory) throws Exception {
        return null;
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(long l, long l1, @Nonnull CheckpointStreamFactory checkpointStreamFactory, @Nonnull CheckpointOptions checkpointOptions) throws Exception {
        return null;
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
