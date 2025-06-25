package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

// TODO: implementation!
public class ForL0StateTableSnapshot<K, N, S> extends AbstractStateTableSnapshot<K, N, S> {

    /**
     * Creates a new {@link AbstractStateTableSnapshot} for and owned by the given table.
     *
     * @param owningStateTable         the {@link StateTable} for which this object represents a snapshot.
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
    }

    @Override
    protected StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup) {
        return null;
    }

    @Override
    public void release() {

    }
}
