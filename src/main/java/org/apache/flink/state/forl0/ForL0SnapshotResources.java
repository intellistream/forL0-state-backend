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
import java.util.TreeMap;

/**
 * Snapshot resources for ForL0 state backend with C++ engine.
 *
 * <p>KV state meta info comes from {@link RegisteredKeyValueStateBackendMetaInfo}, while
 * priority queue snapshots are handled via Flink's standard mechanism.
 *
 * @param <K> The type of key
 */
public class ForL0SnapshotResources<K> implements FullSnapshotResources<K> {

    private ForL0NativeSnapshots.Snapshot nativeSnapshot;

    static final String NATIVE_STATE_ID = "forl0.native-state-id";

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
        this.nativeStateHandles = new HashMap<>(nativeStateHandles);
        this.registeredMetaInfos = new HashMap<>(registeredMetaInfos);
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

        // The native checkpoint payload uses a one-based registration ID.
        // Store it with the state name and keep metadata in that order for
        // readers that register a fresh backend in header order.
        Map<Integer, StateMetaInfoSnapshot> keyedStates = new TreeMap<>();
        for (Map.Entry<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> entry : registeredMetaInfos.entrySet()) {
            int id = NativeEngine.checkpointStateId(nativeStateHandles.get(entry.getKey()));
            StateMetaInfoSnapshot original = entry.getValue().snapshot();
            Map<String, String> options = new HashMap<>(original.getOptionsImmutable());
            options.put(NATIVE_STATE_ID, Integer.toString(id));
            Map<String, TypeSerializer<?>> serializers = new HashMap<>();
            for (String key : original.getSerializerSnapshotsImmutable().keySet()) {
                TypeSerializer<?> serializer = original.getTypeSerializer(key);
                if (serializer != null) serializers.put(key, serializer);
            }
            StateMetaInfoSnapshot snapshot = new StateMetaInfoSnapshot(original.getName(),
                    original.getBackendStateType(), options, original.getSerializerSnapshotsImmutable(), serializers);
            if (keyedStates.put(id, snapshot) != null) throw new IllegalStateException("Duplicate native state ID " + id);
        }
        for (StateMetaInfoSnapshot snapshot : keyedStates.values()) {
            StateUID stateUid = StateUID.of(snapshot.getName(), StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            metaInfoSnapshots.add(snapshot);
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
                pqSnapshots,
                nativeSnapshot == null ? NativeEngine::writeStateKeyGroupEntries : nativeSnapshot::stateGroup);
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

    void attach(ForL0NativeSnapshots.Snapshot snapshot) {
        this.nativeSnapshot = snapshot;
    }

    byte[] checkpointGroup(int group) {
        return nativeSnapshot.checkpointGroup(group);
    }

    void releaseNative() {
        if (nativeSnapshot != null) nativeSnapshot.release();
    }

    @Override
    public void release() {
        releaseNative();
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
