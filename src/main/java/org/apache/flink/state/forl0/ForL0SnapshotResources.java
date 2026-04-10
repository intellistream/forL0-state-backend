package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
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
 * Snapshot resources for ForL0 state backend with C++ engine.
 *
 * <p>KV state meta info comes from {@link RegisteredKeyValueStateBackendMetaInfo}, while
 * priority queue snapshots are handled via Flink's standard mechanism.
 *
 * @param <K> The type of key
 */
public class ForL0SnapshotResources<K> implements FullSnapshotResources<K> {

    private final List<StateMetaInfoSnapshot> metaInfoSnapshots;
    private final Map<StateUID, StateSnapshot> pqSnapshots;
    private final Map<StateUID, Integer> stateNamesToId;
    private final StreamCompressionDecorator streamCompressionDecorator;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final int totalKeyGroups;
    private final Map<String, Long> nativeStateHandles;
    private final Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos;

    private ForL0SnapshotResources(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> pqSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            StreamCompressionDecorator streamCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups,
            Map<String, Long> nativeStateHandles,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos) {
        this.metaInfoSnapshots = metaInfoSnapshots;
        this.pqSnapshots = pqSnapshots;
        this.stateNamesToId = stateNamesToId;
        this.streamCompressionDecorator = streamCompressionDecorator;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.totalKeyGroups = totalKeyGroups;
        this.nativeStateHandles = nativeStateHandles;
        this.registeredMetaInfos = registeredMetaInfos;
    }

    /**
     * Creates snapshot resources from C++ engine meta info and PQ states.
     */
    public static <K> ForL0SnapshotResources<K> create(
            long engineHandle,
            Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            Map<String, Long> nativeStateHandles,
            StreamCompressionDecorator streamCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {

        if (registeredMetaInfos.isEmpty() && registeredPQStates.isEmpty()) {
            return new ForL0SnapshotResources<>(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    streamCompressionDecorator,
                    keyGroupRange,
                    keySerializer,
                    totalKeyGroups,
                    nativeStateHandles,
                    registeredMetaInfos);
        }

        int numStates = registeredMetaInfos.size() + registeredPQStates.size();
        final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
        final Map<StateUID, Integer> stateNamesToId = new HashMap<>(numStates);
        final Map<StateUID, StateSnapshot> pqSnapshots = new HashMap<>();

        // Process KV states — meta info only (actual data is in C++ engine)
        for (Map.Entry<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> entry :
                registeredMetaInfos.entrySet()) {
            StateUID stateUid = StateUID.of(entry.getKey(),
                    StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            metaInfoSnapshots.add(entry.getValue().snapshot());
        }

        // Process priority queue states — full Flink snapshot
        for (Map.Entry<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> entry :
                registeredPQStates.entrySet()) {
            StateUID stateUid = StateUID.of(entry.getKey(),
                    StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);
            stateNamesToId.put(stateUid, stateNamesToId.size());

            StateSnapshot snapshot = entry.getValue().stateSnapshot();
            metaInfoSnapshots.add(snapshot.getMetaInfoSnapshot());
            pqSnapshots.put(stateUid, snapshot);
        }

        return new ForL0SnapshotResources<>(
                metaInfoSnapshots,
                pqSnapshots,
                stateNamesToId,
                streamCompressionDecorator,
                keyGroupRange,
                keySerializer,
                totalKeyGroups,
                nativeStateHandles,
                registeredMetaInfos);
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
                totalKeyGroups,
                keySerializer,
                stateNamesToId,
                nativeStateHandles,
                registeredMetaInfos,
                pqSnapshots);
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
        for (StateSnapshot snapshot : pqSnapshots.values()) {
            snapshot.release();
        }
    }

    /** Returns PQ state snapshots (for standard Flink checkpoint writing). */
    Map<StateUID, StateSnapshot> getPqSnapshots() {
        return pqSnapshots;
    }

    /** Returns the state ID for the given state UID. */
    int getStateId(StateUID stateUid) {
        return stateNamesToId.get(stateUid);
    }

    /** Returns the state names to ID mapping. */
    Map<StateUID, Integer> getStateNamesToId() {
        return stateNamesToId;
    }
}
