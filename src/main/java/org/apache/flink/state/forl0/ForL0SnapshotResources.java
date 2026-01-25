package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Snapshot resources for ForL0 state backend.
 * Implements {@link FullSnapshotResources} to work with Flink's standard checkpoint writers.
 *
 * @param <K> The type of key
 */
public class ForL0SnapshotResources<K> implements FullSnapshotResources<K> {

    private final List<StateMetaInfoSnapshot> metaInfoSnapshots;
    private final Map<StateUID, StateSnapshot> stateSnapshots;
    private final Map<StateUID, Integer> stateNamesToId;
    private final StreamCompressionDecorator streamCompressionDecorator;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final int totalKeyGroups;

    private ForL0SnapshotResources(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> stateSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            StreamCompressionDecorator streamCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {
        this.metaInfoSnapshots = metaInfoSnapshots;
        this.stateSnapshots = stateSnapshots;
        this.stateNamesToId = stateNamesToId;
        this.streamCompressionDecorator = streamCompressionDecorator;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.totalKeyGroups = totalKeyGroups;
    }

    /**
     * Creates snapshot resources from the registered states.
     */
    public static <K> ForL0SnapshotResources<K> create(
            Map<String, ForL0StateStore<K, ?, ?>> registeredStores,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator streamCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {

        if (registeredStores.isEmpty() && registeredPQStates.isEmpty()) {
            return new ForL0SnapshotResources<>(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    streamCompressionDecorator,
                    keyGroupRange,
                    keySerializer,
                    totalKeyGroups);
        }

        int numStates = registeredStores.size() + registeredPQStates.size();
        final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
        final Map<StateUID, Integer> stateNamesToId = new HashMap<>(numStates);
        final Map<StateUID, StateSnapshot> stateSnapshots = new HashMap<>(numStates);

        // Process KV states
        for (Map.Entry<String, ForL0StateStore<K, ?, ?>> entry : registeredStores.entrySet()) {
            StateUID stateUid = StateUID.of(entry.getKey(), StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            
            ForL0StateStore<K, ?, ?> store = entry.getValue();
            StateSnapshot snapshot = store.stateSnapshot();
            metaInfoSnapshots.add(snapshot.getMetaInfoSnapshot());
            stateSnapshots.put(stateUid, snapshot);
        }

        // Process priority queue states
        for (Map.Entry<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> entry : registeredPQStates.entrySet()) {
            StateUID stateUid = StateUID.of(entry.getKey(), StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            
            HeapPriorityQueueSnapshotRestoreWrapper<?> wrapper = entry.getValue();
            StateSnapshot snapshot = wrapper.stateSnapshot();
            metaInfoSnapshots.add(snapshot.getMetaInfoSnapshot());
            stateSnapshots.put(stateUid, snapshot);
        }

        return new ForL0SnapshotResources<>(
                metaInfoSnapshots,
                stateSnapshots,
                stateNamesToId,
                streamCompressionDecorator,
                keyGroupRange,
                keySerializer,
                totalKeyGroups);
    }

    @Override
    @Nonnull
    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return metaInfoSnapshots;
    }

    @Override
    @Nonnull
    public KeyValueStateIterator createKVStateIterator() throws IOException {
        return new ForL0KeyValueStateIterator(
                keyGroupRange,
                keySerializer,
                totalKeyGroups,
                stateNamesToId,
                stateSnapshots);
    }

    @Override
    @Nonnull
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    @Nonnull
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    @Nonnull
    public StreamCompressionDecorator getStreamCompressionDecorator() {
        return streamCompressionDecorator;
    }

    @Override
    public void release() {
        for (StateSnapshot snapshot : stateSnapshots.values()) {
            snapshot.release();
        }
    }

    // Getters for internal use
    Map<StateUID, StateSnapshot> getStateSnapshots() {
        return stateSnapshots;
    }

    Map<StateUID, Integer> getStateNamesToId() {
        return stateNamesToId;
    }
}
