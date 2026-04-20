package org.apache.flink.state.forl0;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
    /** L0 Cache configuration. */
    private final boolean l0CacheEnabled;
    private final long l0CacheSize;
    /** Optional metric group for registering HotCache gauges. May be null in tests. */
    private final MetricGroup metricGroup;

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
            boolean l0CacheEnabled,
            long l0CacheSize,
            CloseableRegistry cancelStreamRegistry) {
        this(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups,
                keyGroupRange, executionConfig, ttlTimeProvider, latencyTrackingStateConfig,
                stateHandles, keyGroupCompressionDecorator, priorityQueueSetFactory,
                asynchronousSnapshots, l0CacheEnabled, l0CacheSize, cancelStreamRegistry,
                /* metricGroup */ null);
    }

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
            boolean l0CacheEnabled,
            long l0CacheSize,
            CloseableRegistry cancelStreamRegistry,
            MetricGroup metricGroup) {
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
        this.l0CacheEnabled = l0CacheEnabled;
        this.l0CacheSize = l0CacheSize;
        this.metricGroup = metricGroup;
    }

    @Override
    public ForL0KeyedStateBackend<K> build() throws BackendBuildingException {
        LOG.info("[ForL0] Building ForL0KeyedStateBackend with C++ engine...");

        // Load native library
        NativeEngine.ensureLoaded();

        // Create C++ engine
        int startKeyGroup = keyGroupRange.getStartKeyGroup();
        int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        long engineHandle = NativeEngine.createEngine(
                startKeyGroup, numKeyGroups, numberOfKeyGroups,
                l0CacheEnabled, l0CacheSize);
        LOG.info("[ForL0] C++ engine created: handle={}, keyGroups=[{}, {}), total={}, l0Enabled={}",
                engineHandle, startKeyGroup, startKeyGroup + numKeyGroups, numberOfKeyGroups,
                l0CacheEnabled);

        // Register HotCache gauges on the provided MetricGroup (design §8).
        // Always register so users can see whether the cache actually came up.
        if (metricGroup != null) {
            registerHotCacheMetrics(metricGroup, engineHandle);
        }

        // Native state handles (populated lazily by backend on first state registration)
        Map<String, Long> nativeStateHandles = new HashMap<>();
        // Meta info registry for checkpoint metadata
        Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos = new HashMap<>();
        // Priority queue states
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates = new HashMap<>();

        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();

        ForL0KeyContext<K> keyContext =
                new ForL0KeyContext<>(keyGroupRange, numberOfKeyGroups);

        // Restore state if any
        if (!restoreStateHandles.isEmpty()) {
            try {
                restoreFromHandles(
                        engineHandle,
                        nativeStateHandles,
                        registeredMetaInfos,
                        registeredPQStates);
            } catch (Exception e) {
                // Destroy the engine on restore failure to avoid native memory leak
                NativeEngine.destroyEngine(engineHandle);
                throw new BackendBuildingException("Failed to restore ForL0 state.", e);
            }
        }

        // Initialize snapshot strategy
        ForL0SnapshotStrategy<K> snapshotStrategy = new ForL0SnapshotStrategy<>(
                engineHandle,
                registeredMetaInfos,
                registeredPQStates,
                nativeStateHandles,
                keyGroupCompressionDecorator,
                keyGroupRange,
                keySerializerProvider.currentSchemaSerializer(),
                numberOfKeyGroups);

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
                engineHandle,
                nativeStateHandles,
                registeredMetaInfos,
                registeredPQStates,
                priorityQueueSetFactory,
                snapshotStrategy,
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS,
                keyContext);
    }

    // ========================================================================
    //  State restore from checkpoint
    // ========================================================================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void restoreFromHandles(
            long engineHandle,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates)
            throws Exception {

        boolean keySerializerRestored = false;

        for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
            if (keyedStateHandle == null) {
                continue;
            }
            if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                throw new BackendBuildingException(
                        "Unexpected state handle type: " + keyedStateHandle.getClass().getName());
            }

            // Canonical savepoint format (FullSnapshotAsyncWriter) has a different
            // binary layout than our custom checkpoint format.
            if (keyedStateHandle instanceof KeyGroupsSavepointStateHandle) {
                keySerializerRestored = restoreFromCanonicalSavepoint(
                        (KeyGroupsSavepointStateHandle) keyedStateHandle,
                        engineHandle, nativeStateHandles, registeredMetaInfos,
                        registeredPQStates, keySerializerRestored);
                continue;
            }

            LOG.info("[ForL0] Restoring from state handle: {}", keyedStateHandle);
            KeyGroupsStateHandle kgsHandle = (KeyGroupsStateHandle) keyedStateHandle;
            FSDataInputStream fsDataInputStream = kgsHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(fsDataInputStream);

            try {
                DataInputViewStreamWrapper inView =
                        new DataInputViewStreamWrapper(fsDataInputStream);

                // Read checkpoint metadata
                KeyedBackendSerializationProxy<K> serProxy =
                        new KeyedBackendSerializationProxy<>(userCodeClassLoader);
                serProxy.read(inView);

                // Validate key serializer compatibility (once across all handles)
                if (!keySerializerRestored) {
                    TypeSerializer<K> currentSerializer =
                            keySerializerProvider.currentSchemaSerializer();
                    TypeSerializerSchemaCompatibility<K> compat =
                            keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                                    serProxy.getKeySerializerSnapshot());
                    if (compat.isCompatibleAfterMigration() || compat.isIncompatible()) {
                        throw new StateMigrationException(
                                "The new key serializer ("
                                        + currentSerializer
                                        + ") must be compatible with the previous key serializer ("
                                        + keySerializerProvider.previousSchemaSerializer()
                                        + ").");
                    }
                    keySerializerRestored = true;
                }

                List<StateMetaInfoSnapshot> metaInfoSnapshots =
                        serProxy.getStateMetaInfoSnapshots();

                // Classify states and register them
                Map<Integer, StateMetaInfoSnapshot> statesById = new HashMap<>();
                int pqStateCount = 0;

                for (int i = 0; i < metaInfoSnapshots.size(); i++) {
                    StateMetaInfoSnapshot metaInfo = metaInfoSnapshots.get(i);
                    statesById.put(i, metaInfo);

                    switch (metaInfo.getBackendStateType()) {
                        case KEY_VALUE:
                            if (!nativeStateHandles.containsKey(metaInfo.getName())) {
                                registerKVStateFromMetaInfo(
                                        engineHandle,
                                        metaInfo,
                                        nativeStateHandles,
                                        registeredMetaInfos);
                            }
                            break;
                        case PRIORITY_QUEUE:
                            pqStateCount++;
                            if (!registeredPQStates.containsKey(metaInfo.getName())) {
                                createPQWrapper(metaInfo, registeredPQStates);
                            }
                            break;
                        default:
                            throw new BackendBuildingException(
                                    "Unexpected state type: " + metaInfo.getBackendStateType());
                    }
                }

                // Restore key group data
                StreamCompressionDecorator compressionDecorator =
                        serProxy.isUsingKeyGroupCompression()
                                ? SnappyStreamCompressionDecorator.INSTANCE
                                : UncompressedStreamCompressionDecorator.INSTANCE;

                for (Tuple2<Integer, Long> groupOffset : kgsHandle.getGroupRangeOffsets()) {
                    int keyGroupId = groupOffset.f0;
                    long offset = groupOffset.f1;

                    if (!keyGroupRange.contains(keyGroupId)) {
                        continue;
                    }

                    fsDataInputStream.seek(offset);
                    int writtenKG = inView.readInt();
                    Preconditions.checkState(
                            writtenKG == keyGroupId,
                            "Unexpected key-group in restore: expected %s, got %s",
                            keyGroupId,
                            writtenKG);

                    // Read PQ state blocks (each PQ state in its own compression block)
                    for (int i = 0; i < pqStateCount; i++) {
                        try (InputStream kgIn =
                                compressionDecorator.decorateWithCompression(fsDataInputStream)) {
                            DataInputViewStreamWrapper kgInView =
                                    new DataInputViewStreamWrapper(kgIn);
                            int stateId = kgInView.readShort();
                            StateMetaInfoSnapshot meta = statesById.get(stateId);
                            HeapPriorityQueueSnapshotRestoreWrapper<?> pqWrapper =
                                    registeredPQStates.get(meta.getName());
                            pqWrapper.keyGroupReader(serProxy.getReadVersion())
                                    .readMappingsInKeyGroup(kgInView, keyGroupId);
                        }
                    }

                    // Read KV state block (always present — see snapshot strategy)
                    try (InputStream kgIn =
                            compressionDecorator.decorateWithCompression(fsDataInputStream)) {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] buf = new byte[4096];
                        int len;
                        while ((len = kgIn.read(buf)) != -1) {
                            baos.write(buf, 0, len);
                        }
                        byte[] kvData = baos.toByteArray();
                        if (kvData.length > 0) {
                            NativeEngine.readKeyGroupData(engineHandle, keyGroupId, kvData);
                        }
                    }
                }

                LOG.info("[ForL0] Restored from state handle: {}", keyedStateHandle);
            } finally {
                if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
                    IOUtils.closeQuietly(fsDataInputStream);
                }
            }
        }
    }

    // ========================================================================
    //  Canonical savepoint restore (FullSnapshotAsyncWriter format)
    // ========================================================================

    private static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

    private static boolean hasMetaDataFollowsFlag(byte[] key) {
        return key.length > 0 && (key[0] & 0x80) != 0;
    }

    private static void clearMetaDataFollowsFlag(byte[] key) {
        if (key.length > 0) {
            key[0] &= (byte) ~0x80;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean restoreFromCanonicalSavepoint(
            KeyGroupsSavepointStateHandle savepointHandle,
            long engineHandle,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            boolean keySerializerRestored) throws Exception {

        LOG.info("[ForL0] Restoring from canonical savepoint: {}", savepointHandle);
        FSDataInputStream fsDataInputStream = savepointHandle.openInputStream();
        cancelStreamRegistry.registerCloseable(fsDataInputStream);

        try {
            DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

            KeyedBackendSerializationProxy<K> serProxy =
                    new KeyedBackendSerializationProxy<>(userCodeClassLoader);
            serProxy.read(inView);

            if (!keySerializerRestored) {
                TypeSerializerSchemaCompatibility<K> compat =
                        keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                                serProxy.getKeySerializerSnapshot());
                if (compat.isCompatibleAfterMigration() || compat.isIncompatible()) {
                    throw new StateMigrationException(
                            "Key serializer incompatible for savepoint restore.");
                }
                keySerializerRestored = true;
            }

            List<StateMetaInfoSnapshot> metaInfoSnapshots = serProxy.getStateMetaInfoSnapshots();

            // Register all states
            for (int i = 0; i < metaInfoSnapshots.size(); i++) {
                StateMetaInfoSnapshot metaInfo = metaInfoSnapshots.get(i);
                switch (metaInfo.getBackendStateType()) {
                    case KEY_VALUE:
                        if (!nativeStateHandles.containsKey(metaInfo.getName())) {
                            registerKVStateFromMetaInfo(
                                    engineHandle, metaInfo, nativeStateHandles, registeredMetaInfos);
                        }
                        break;
                    case PRIORITY_QUEUE:
                        if (!registeredPQStates.containsKey(metaInfo.getName())) {
                            createPQWrapper(metaInfo, registeredPQStates);
                        }
                        break;
                }
            }

            StreamCompressionDecorator compressionDecorator =
                    serProxy.isUsingKeyGroupCompression()
                            ? SnappyStreamCompressionDecorator.INSTANCE
                            : UncompressedStreamCompressionDecorator.INSTANCE;

            int keyGroupPrefixBytes = CompositeKeySerializationUtils
                    .computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
            TypeSerializer<K> keySerializer = keySerializerProvider.currentSchemaSerializer();

            for (Tuple2<Integer, Long> groupOffset : savepointHandle.getGroupRangeOffsets()) {
                int keyGroupId = groupOffset.f0;
                long offset = groupOffset.f1;

                if (!keyGroupRange.contains(keyGroupId) || offset == 0L) continue;

                // Canonical format: offset points directly to compression block (no keyGroupId int)
                fsDataInputStream.seek(offset);

                try (InputStream kgIn =
                        compressionDecorator.decorateWithCompression(fsDataInputStream)) {
                    DataInputViewStreamWrapper kgInView = new DataInputViewStreamWrapper(kgIn);

                    int currentStateId = Short.toUnsignedInt(kgInView.readShort());

                    while (currentStateId != END_OF_KEY_GROUP_MARK) {
                        StateMetaInfoSnapshot meta = metaInfoSnapshots.get(currentStateId);
                        String stateName = meta.getName();

                        // Collect entries until meta-data-follows flag
                        List<byte[]> compositeKeys = new ArrayList<>();
                        List<byte[]> values = new ArrayList<>();

                        boolean lastEntry = false;
                        while (!lastEntry) {
                            byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInView);
                            byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInView);

                            if (hasMetaDataFollowsFlag(key)) {
                                clearMetaDataFollowsFlag(key);
                                lastEntry = true;
                            }
                            compositeKeys.add(key);
                            values.add(value);
                        }

                        // Read next state ID or end marker
                        currentStateId = Short.toUnsignedInt(kgInView.readShort());

                        // Dispatch based on state type
                        if (meta.getBackendStateType() ==
                                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                            restoreCanonicalKVEntries(
                                    engineHandle, keyGroupId, stateName,
                                    compositeKeys, values,
                                    keySerializer, keyGroupPrefixBytes,
                                    nativeStateHandles, registeredMetaInfos);
                        }
                        // PQ state restore from canonical format: TODO if needed
                    }
                }
            }

            LOG.info("[ForL0] Restored canonical savepoint: {}", savepointHandle);
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
                IOUtils.closeQuietly(fsDataInputStream);
            }
        }
        return keySerializerRestored;
    }

    /**
     * Restore KV state entries from canonical savepoint format.
     * Converts BytePrimitive composite keys + values to our C++ binary format
     * and feeds them to the C++ engine via readStateKeyGroupEntries.
     */
    @SuppressWarnings("unchecked")
    private void restoreCanonicalKVEntries(
            long engineHandle,
            int keyGroupId,
            String stateName,
            List<byte[]> compositeKeys,
            List<byte[]> values,
            TypeSerializer<K> keySerializer,
            int keyGroupPrefixBytes,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos)
            throws Exception {

        Long stateHandle = nativeStateHandles.get(stateName);
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo = registeredMetaInfos.get(stateName);
        if (stateHandle == null || metaInfo == null) return;

        // Determine stored binary format (must match C++ registration logic)
        int keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        int valueTypeId = TypeAnalyzer.getTypeId(metaInfo.getStateSerializer());
        boolean isValue = metaInfo.getStateType() == StateDescriptor.Type.VALUE;
        boolean voidNs = metaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer;

        // Key format: 8=fixed8, 4=fixed4, 0=len-prefixed
        // Value format: 8=fixed8, 0=len-prefixed
        int keyFmt, valFmt;
        if (isValue) {
            boolean numericValue = (valueTypeId == TypeAnalyzer.TYPE_INT64
                    || valueTypeId == TypeAnalyzer.TYPE_INT32
                    || valueTypeId == TypeAnalyzer.TYPE_FLOAT64);
            if (keyTypeId == TypeAnalyzer.TYPE_INT64) {
                keyFmt = 8;
                valFmt = numericValue ? 8 : 0;
            } else if (keyTypeId == TypeAnalyzer.TYPE_INT32) {
                keyFmt = 4;
                valFmt = numericValue ? 8 : 0;
            } else if (keyTypeId == TypeAnalyzer.TYPE_FIXED_ROW) {
                keyFmt = 0;
                valFmt = numericValue ? 8 : 0;
            } else {
                keyFmt = 0;
                valFmt = 0;
            }
        } else {
            // LIST, MAP, REDUCING, AGGREGATING
            keyFmt = (keyTypeId == TypeAnalyzer.TYPE_INT64) ? 8 : 0;
            boolean isReducing = (metaInfo.getStateType() == StateDescriptor.Type.REDUCING);
            if (isReducing && keyTypeId == TypeAnalyzer.TYPE_INT64
                    && valueTypeId == TypeAnalyzer.TYPE_INT64) {
                valFmt = 8;
            } else {
                valFmt = 0;
            }
        }

        // Build binary data: [count(4)][entries: ns+key+value]
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(compositeKeys.size());

        for (int i = 0; i < compositeKeys.size(); i++) {
            byte[] compositeKey = compositeKeys.get(i);
            byte[] value = values.get(i);

            // Decompose composite key: [keyGroupPrefix][serializedKey][serializedNamespace]
            DataInputDeserializer dis = new DataInputDeserializer(
                    compositeKey, keyGroupPrefixBytes,
                    compositeKey.length - keyGroupPrefixBytes);
            ((TypeSerializer<Object>) keySerializer).deserialize(dis);
            int keyEndAbsPos = dis.getPosition();

            byte[] rawKeyBytes = Arrays.copyOfRange(compositeKey, keyGroupPrefixBytes, keyEndAbsPos);
            byte[] rawNsBytes = Arrays.copyOfRange(compositeKey, keyEndAbsPos, compositeKey.length);

            // Write namespace
            if (voidNs) {
                dos.write(rawNsBytes);
            } else {
                dos.writeInt(rawNsBytes.length);
                dos.write(rawNsBytes);
            }

            // Write key in stored format
            if (keyFmt > 0) {
                dos.write(rawKeyBytes);
            } else {
                dos.writeInt(rawKeyBytes.length);
                dos.write(rawKeyBytes);
            }

            // Write value in stored format
            if (valFmt > 0) {
                dos.write(value);
            } else {
                dos.writeInt(value.length);
                dos.write(value);
            }
        }

        dos.flush();
        byte[] binaryData = baos.toByteArray();
        NativeEngine.readStateKeyGroupEntries(stateHandle, keyGroupId, binaryData);
    }

    /**
     * Register a KV state in the C++ engine from checkpoint metadata.
     */
    private void registerKVStateFromMetaInfo(
            long engineHandle,
            StateMetaInfoSnapshot metaInfo,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos) {

        RegisteredKeyValueStateBackendMetaInfo<?, ?> kvMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(metaInfo);
        registeredMetaInfos.put(metaInfo.getName(), kvMetaInfo);

        // Map Flink StateDescriptor.Type to native stateType int
        int stateType;
        switch (kvMetaInfo.getStateType()) {
            case VALUE:      stateType = 0; break;
            case LIST:       stateType = 1; break;
            case MAP:        stateType = 2; break;
            case REDUCING:   stateType = 3; break;
            case AGGREGATING: stateType = 4; break;
            default:
                throw new IllegalStateException(
                        "Unsupported state type for restore: " + kvMetaInfo.getStateType());
        }

        TypeSerializer<?> keySerializer = keySerializerProvider.currentSchemaSerializer();
        TypeSerializer<?> valueSerializer = kvMetaInfo.getStateSerializer();
        TypeSerializer<?> nsSerializer = kvMetaInfo.getNamespaceSerializer();

        int keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        int valueTypeId = TypeAnalyzer.getTypeId(valueSerializer);
        int nsTypeId = TypeAnalyzer.isVoidNamespace(nsSerializer)
                ? TypeAnalyzer.TYPE_VOID_NS
                : TypeAnalyzer.getTypeId(nsSerializer);

        byte[] typeDescriptor =
                TypeAnalyzer.generateStateDescriptor(keySerializer, nsSerializer, valueSerializer);

        long handle = NativeEngine.registerState(
                engineHandle,
                metaInfo.getName(),
                stateType,
                keyTypeId,
                valueTypeId,
                nsTypeId,
                typeDescriptor);

        nativeStateHandles.put(metaInfo.getName(), handle);
        LOG.info("[ForL0] Restored KV state '{}' type={}, handle={}",
                metaInfo.getName(), kvMetaInfo.getStateType(), handle);
    }

    /**
     * Create a HeapPriorityQueueSnapshotRestoreWrapper from checkpoint metadata.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createPQWrapper(
            StateMetaInfoSnapshot metaInfo,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {

        RegisteredPriorityQueueStateBackendMetaInfo restoredMetaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(metaInfo);

        HeapPriorityQueueSet priorityQueue = priorityQueueSetFactory.create(
                metaInfo.getName(), restoredMetaInfo.getElementSerializer());

        HeapPriorityQueueSnapshotRestoreWrapper wrapper =
                new HeapPriorityQueueSnapshotRestoreWrapper<>(
                        priorityQueue,
                        restoredMetaInfo,
                        org.apache.flink.runtime.state.KeyExtractorFunction.forKeyedObjects(),
                        keyGroupRange,
                        numberOfKeyGroups);

        registeredPQStates.put(metaInfo.getName(), wrapper);
        LOG.info("[ForL0] Restored PQ state '{}'", metaInfo.getName());
    }

    // -----------------------------------------------------------------------
    //  HotCache metrics registration (design §8).
    //
    //  Always expose manager-level gauges so operators can tell whether their
    //  `l0-cache.enabled=true` request actually resolved to an active cache —
    //  especially important when hardware gating forces a fallback to
    //  disabled (bytes_capacity == 0, active == 0).
    // -----------------------------------------------------------------------
    private static void registerHotCacheMetrics(MetricGroup parent, long engineHandle) {
        try {
            MetricGroup g = parent.addGroup("forl0").addGroup("hotcache");
            // Snapshot cache every query; the values are atomic counters / plain
            // longs in C++. Nine-slot scratch buffer avoids per-call allocation.
            final long[] buf = new long[9];
            g.gauge("active",             () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[0]; });
            g.gauge("bytesCapacity",      () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[1]; });
            g.gauge("bytesUsed",          () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[2]; });
            g.gauge("totalSets",          () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[3]; });
            g.gauge("freeSets",           () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[4]; });
            g.gauge("lookups",            () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[5]; });
            g.gauge("hits",               () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[6]; });
            g.gauge("invalidations",      () -> { NativeEngine.getHotCacheManagerStats(engineHandle, buf); return buf[7]; });
        } catch (Throwable t) {
            // Metric registration is best-effort; don't fail backend build.
            LOG.warn("[ForL0] Failed to register HotCache metrics: {}", t.getMessage());
        }
    }
}
