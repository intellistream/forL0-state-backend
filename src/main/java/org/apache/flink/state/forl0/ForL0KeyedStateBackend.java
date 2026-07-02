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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that delegates all state storage to the
 * C++ ForL0 engine via JNI. This is a thin shell — all state data lives in
 * off-heap memory managed by C++.
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

    /** Native C++ engine handle. */
    private final long engineHandle;

    /** Whether this backend has been disposed. */
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    /** Map of state name → native state handle (long). */
    private final Map<String, Long> nativeStateHandles;

    /** Map of created State objects. */
    private final Map<String, State> createdKVStates;

    /** Meta info registry for checkpoint compatibility. */
    private final Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos;

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
            long engineHandle,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
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
        this.engineHandle = engineHandle;
        this.nativeStateHandles = nativeStateHandles;
        this.registeredMetaInfos = registeredMetaInfos;
        this.createdKVStates = new HashMap<>();
        this.snapshotStrategy = snapshotStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());
        LOG.info("[ForL0] Initializing ForL0 keyed state backend with C++ engine (handle={})", engineHandle);
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
    private <N, V> RegisteredKeyValueStateBackendMetaInfo<N, V> tryRegisterMetaInfo(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws StateMigrationException {

        RegisteredKeyValueStateBackendMetaInfo<N, V> metaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<N, V>) registeredMetaInfos.get(stateDesc.getName());

        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (metaInfo != null) {
            metaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            TypeSerializer<N> previousNamespaceSerializer = metaInfo.getNamespaceSerializer();
            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    metaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For ForL0 backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }

            metaInfo.checkStateMetaInfo(stateDesc);

            TypeSerializer<V> previousStateSerializer = metaInfo.getStateSerializer();
            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    metaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For ForL0 backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }

            metaInfo = allowFutureMetadataUpdates
                    ? metaInfo.withSerializerUpgradesAllowed()
                    : metaInfo;
        } else {
            metaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
                    stateDesc.getType(),
                    stateDesc.getName(),
                    namespaceSerializer,
                    newStateSerializer,
                    snapshotTransformFactory);

            metaInfo = allowFutureMetadataUpdates
                    ? metaInfo.withSerializerUpgradesAllowed()
                    : metaInfo;

            registeredMetaInfos.put(stateDesc.getName(), metaInfo);
        }

        return metaInfo;
    }

    /**
     * Ensure a native state table exists for the given state.
     * Registers with C++ engine on first use.
     */
    private <N, V> long ensureNativeState(
            String stateName,
            StateDescriptor<?, V> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> stateSerializer) {

        Long existingHandle = nativeStateHandles.get(stateName);
        if (existingHandle != null) {
            return existingHandle;
        }

        int stateType;
        switch (stateDesc.getType()) {
            case VALUE: stateType = 0; break;
            case LIST: stateType = 1; break;
            case MAP: stateType = 2; break;
            case REDUCING: stateType = 3; break;
            case AGGREGATING: stateType = 4; break;
            default:
                throw new FlinkRuntimeException("Unsupported state type: " + stateDesc.getType());
        }

        int keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        int valueTypeId = TypeAnalyzer.getTypeId(stateSerializer);
        int nsTypeId = TypeAnalyzer.getTypeId(namespaceSerializer);
        byte[] typeDescriptor = TypeAnalyzer.generateStateDescriptor(
                keySerializer, namespaceSerializer, stateSerializer);

        long handle = NativeEngine.registerState(
                engineHandle, stateName, stateType,
                keyTypeId, valueTypeId, nsTypeId,
                typeDescriptor);

        nativeStateHandles.put(stateName, handle);
        LOG.debug("[ForL0] Registered native state '{}' type={} keyTypeId={} valueTypeId={} nsTypeId={} handle={}",
                stateName, stateType, keyTypeId, valueTypeId, nsTypeId, handle);
        return handle;
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        Long handle = nativeStateHandles.get(state);
        if (handle == null) return Stream.empty();
        return decodeKeysFromNative(NativeEngine.getStateKeys(handle));
    }

    @Override
    public <N> Stream<K> getKeys(List<String> states, N namespace) {
        return states.stream().flatMap(s -> getKeys(s, namespace));
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        // VoidNamespace is the common case; return keys paired with null namespace.
        // For general namespace mode, full iteration is not yet supported.
        return getKeys(state, null).map(k -> Tuple2.of(k, null));
    }

    @SuppressWarnings("unchecked")
    private <T> Stream<T> decodeKeysFromNative(byte[] data) {
        if (data == null || data.length <= 4) return Stream.empty();
        ByteBuffer bb = ByteBuffer.wrap(data);
        int count = bb.getInt();
        if (count <= 0) return Stream.empty();

        int keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        List<T> keys = new ArrayList<>(count);
        try {
            if (keyTypeId == TypeAnalyzer.TYPE_INT64) {
                for (int i = 0; i < count; i++) {
                    keys.add((T) Long.valueOf(bb.getLong()));
                }
            } else if (keyTypeId == TypeAnalyzer.TYPE_INT32) {
                for (int i = 0; i < count; i++) {
                    keys.add((T) Integer.valueOf(bb.getInt()));
                }
            } else {
                // String/bytes keys: [length(int)] + [bytes] each
                org.apache.flink.core.memory.DataInputDeserializer in =
                        new org.apache.flink.core.memory.DataInputDeserializer(data, 4, data.length - 4);
                for (int i = 0; i < count; i++) {
                    int len = in.readInt();
                    byte[] keyBytes = new byte[len];
                    in.readFully(keyBytes);
                    // Deserialize the key using the key serializer
                    org.apache.flink.core.memory.DataInputDeserializer keyIn =
                            new org.apache.flink.core.memory.DataInputDeserializer(keyBytes);
                    keys.add((T) keySerializer.deserialize(keyIn));
                }
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to decode keys from native engine", e);
        }
        return keys.stream();
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

        RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo =
                tryRegisterMetaInfo(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory),
                        allowFutureMetadataUpdates);

        long stateHandle = ensureNativeState(
                stateDesc.getName(), stateDesc,
                metaInfo.getNamespaceSerializer(),
                metaInfo.getStateSerializer());

        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        if (createdState == null) {
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateCreateFactory.createState(
                    stateDesc, stateHandle,
                    metaInfo.getNamespaceSerializer(),
                    metaInfo.getStateSerializer(),
                    this);
        } else {
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            createdState = stateUpdateFactory.updateState(
                    stateDesc,
                    metaInfo.getNamespaceSerializer(),
                    metaInfo.getStateSerializer(),
                    createdState);
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
    public SavepointResources<K> savepoint() throws Exception {
        ForL0SnapshotResources<K> snapshotResources =
                snapshotStrategy.syncPrepareResources(-1L);

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

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        return (int) NativeEngine.totalEntries(engineHandle);
    }

    @Override
    public void dispose() {
        if (!disposed.compareAndSet(false, true)) {
            return;
        }
        super.dispose();
        NativeEngine.destroyEngine(engineHandle);
        LOG.info("[ForL0] C++ engine destroyed (handle={})", engineHandle);
    }

    /** Returns the native engine handle. */
    long getEngineHandle() {
        return engineHandle;
    }

    /** Returns the native state handles map. */
    Map<String, Long> getNativeStateHandles() {
        return nativeStateHandles;
    }

    /** Returns the registered meta infos. */
    Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> getRegisteredMetaInfos() {
        return registeredMetaInfos;
    }

    /**
     * Returns the ForL0KeyContext for direct field access in State implementations.
     */
    ForL0KeyContext<K> getForL0KeyContext() {
        return forl0KeyContext;
    }

    // ========== Factory Interfaces ==========

    interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                long stateHandle,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<SV> stateSerializer,
                ForL0KeyedStateBackend<K> backend)
                throws Exception;
    }

    interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<SV> stateSerializer,
                IS existingState)
                throws Exception;
    }
}
