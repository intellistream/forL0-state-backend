package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createSimpleStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/**
 * Snapshot strategy for ForL0 state backend with C++ engine.
 *
 * <p>KV state data is serialized from the C++ engine via JNI.
 * Priority queue states are handled by Flink's standard mechanism.
 *
 * @param <K> The type of key
 */
public class ForL0SnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, ForL0SnapshotResources<K>> {

    private final long engineHandle;
    private final Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final Map<String, Long> nativeStateHandles;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final int totalKeyGroups;

    public ForL0SnapshotStrategy(
            long engineHandle,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            Map<String, Long> nativeStateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {
        this.engineHandle = engineHandle;
        this.registeredMetaInfos = registeredMetaInfos;
        this.registeredPQStates = registeredPQStates;
        this.nativeStateHandles = nativeStateHandles;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.totalKeyGroups = totalKeyGroups;
    }

    ForL0SnapshotResources<K> createSnapshotResources() {
        return ForL0SnapshotResources.create(
                engineHandle,
                registeredMetaInfos,
                registeredPQStates,
                nativeStateHandles,
                keyGroupCompressionDecorator,
                keyGroupRange,
                keySerializer,
                totalKeyGroups);
    }

    @Override
    public ForL0SnapshotResources<K> syncPrepareResources(long checkpointId) throws Exception {
        // Prepare C++ engine for snapshot (freeze consistent state)
        NativeEngine.prepareSnapshot(engineHandle);
        return createSnapshotResources();
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            ForL0SnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        List<StateMetaInfoSnapshot> metaInfoSnapshots = syncPartResource.getMetaInfoSnapshots();
        if (metaInfoSnapshots.isEmpty()) {
            return snapshotCloseableRegistry -> SnapshotResult.empty();
        }

        final KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        syncPartResource.getKeySerializer(),
                        metaInfoSnapshots,
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                keyGroupCompressionDecorator));

        return (snapshotCloseableRegistry) -> {
            final CheckpointStreamWithResultProvider streamWithResultProvider =
                    createSimpleStream(CheckpointedStateScope.EXCLUSIVE, streamFactory);

            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

            final CheckpointStateOutputStream localStream =
                    streamWithResultProvider.getCheckpointOutputStream();

            final DataOutputViewStreamWrapper outView =
                    new DataOutputViewStreamWrapper(localStream);

            // Write metadata (serialization proxy)
            serializationProxy.write(outView);

            final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

            // Write each key group's data
            for (int keyGroupPos = 0;
                    keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                    ++keyGroupPos) {
                int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);

                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
                outView.writeInt(keyGroupId);

                // Write PQ state data for this key group using the standard Flink mechanism
                Map<StateUID, StateSnapshot> pqSnapshots = syncPartResource.getPqSnapshots();
                for (Map.Entry<StateUID, StateSnapshot> entry : pqSnapshots.entrySet()) {
                    StateSnapshot.StateKeyGroupWriter writer = entry.getValue().getKeyGroupWriter();
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);
                        kgCompressionView.writeShort(syncPartResource.getStateId(entry.getKey()));
                        writer.writeStateInKeyGroup(kgCompressionView, keyGroupId);
                    }
                }

                // Write KV state data from C++ engine for this key group.
                // Always write a compression block (even if empty) so restore can rely on
                // the fixed block count: numPQStates + 1 blocks per key group.
                byte[] keyGroupData = NativeEngine.writeKeyGroupData(engineHandle, keyGroupId);
                try (OutputStream kgCompressionOut =
                        keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                    if (keyGroupData != null && keyGroupData.length > 0) {
                        kgCompressionOut.write(keyGroupData);
                    }
                }
            }

            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                KeyGroupRangeOffsets kgOffs =
                        new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
            } else {
                throw new IOException("Stream already unregistered.");
            }
        };
    }
}
