package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.levelhash.LevelHashStateMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

// TODO: implementation!
public class ForL0StateMapSnapshot<K, N, S>
        extends StateMapSnapshot<K, N, S, ForL0StateMap<K, N, S>> {

    public ForL0StateMapSnapshot(ForL0StateMap<K, N, S> stateMap) {
        super(stateMap);
    }

    @Override
    public Iterator<StateEntry<K, N, S>> getIterator(@Nonnull TypeSerializer<K> keySerializer, @Nonnull TypeSerializer<N> namespaceSerializer, @Nonnull TypeSerializer<S> stateSerializer, @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
        return null;
    }

    @Override
    public void writeState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, TypeSerializer<S> stateSerializer, @Nonnull DataOutputView dov, @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {

    }
}
