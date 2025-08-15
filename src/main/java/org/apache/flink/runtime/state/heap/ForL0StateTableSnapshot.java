package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import java.util.List;

/**
 * ForL0StateTableSnapshot that ensures compatibility with standard HeapStateBackend format.
 * This class extends AbstractStateTableSnapshot and uses the same data serialization format
 * as CopyOnWriteStateTableSnapshot to ensure checkpoint compatibility.
 */
public class ForL0StateTableSnapshot<K, N, S> extends AbstractStateTableSnapshot<K, N, S> {

    private final List<ForL0StateMapSnapshot<K, N, S>> perKeyGroupSnapshots;

    /**
     * Creates a new {@link ForL0StateTableSnapshot} for and owned by the given table.
     *
     * @param owningStateTable         the {@link StateTable} for which this object represents a snapshot.
     * @param localKeySerializer       the key serializer
     * @param localNamespaceSerializer the namespace serializer
     * @param localStateSerializer     the state serializer
     * @param stateSnapshotTransformer optional state transformation
     */
    ForL0StateTableSnapshot(StateTable<K, N, S> owningStateTable,
                            TypeSerializer<K> localKeySerializer,
                            TypeSerializer<N> localNamespaceSerializer,
                            TypeSerializer<S> localStateSerializer,
                            StateSnapshotTransformer<S> stateSnapshotTransformer) {
        super(
                owningStateTable,
                localKeySerializer,
                localNamespaceSerializer,
                localStateSerializer,
                stateSnapshotTransformer);

        // Collect snapshots from all key groups in the same format as HeapStateBackend
        this.perKeyGroupSnapshots = ((ForL0StateTable<K, N, S>) owningStateTable).getStateMapSnapshotList();
    }

    @Override
    protected StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup) {
        // Convert global key group ID to local index using the correct method
        // The keyGroup parameter is already the local index when called from AbstractStateTableSnapshot
        if (keyGroup < 0 || keyGroup >= perKeyGroupSnapshots.size()) {
            // Return null for key groups not owned by this backend (standard behavior)
            return null;
        }

        return perKeyGroupSnapshots.get(keyGroup);
    }

    @Override
    public void release() {
        // Clean up references to help GC
        if (perKeyGroupSnapshots != null) {
            for (ForL0StateMapSnapshot<K, N, S> snapshot : perKeyGroupSnapshots) {
                if (snapshot != null) {
                    snapshot.release();
                }
            }
            perKeyGroupSnapshots.clear();
        }
    }
}
