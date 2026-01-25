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
 * Snapshot strategy for ForL0 state backend.
 * 
 * <p>This uses the same checkpoint format as HeapSnapshotStrategy for compatibility
 * with HeapRestoreOperation.
 *
 * @param <K> The type of key
 */
public class ForL0SnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, ForL0SnapshotResources<K>> {

    private final Map<String, ForL0StateStore<K, ?, ?>> registeredStores;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final int totalKeyGroups;

    public ForL0SnapshotStrategy(
            Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {
        this.registeredStores = registeredStores;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public ForL0SnapshotResources<K> syncPrepareResources(long checkpointId) throws Exception {
        return ForL0SnapshotResources.create(
                registeredStores,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                keySerializer,
                totalKeyGroups);
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

        // Create serialization proxy (same as HeapSnapshotStrategy)
        final KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        syncPartResource.getKeySerializer(),
                        metaInfoSnapshots,
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                keyGroupCompressionDecorator));

        return (snapshotCloseableRegistry) -> {
            final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
            final Map<StateUID, StateSnapshot> stateSnapshots = syncPartResource.getStateSnapshots();
            
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
                
                // Record offset BEFORE writing key group ID
                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
                
                // Write key group ID
                outView.writeInt(keyGroupId);

                // Write each state's data for this key group
                for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                        stateSnapshots.entrySet()) {
                    StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
                            stateSnapshot.getValue().getKeyGroupWriter();
                    
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);
                        // Write state ID
                        kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
                        // Write state data
                        partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
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
