package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

public class ForL0StateMapSnapshot<K, N, S>
        extends StateMapSnapshot<K, N, S, ForL0StateMap<K, N, S>> {

    private final ForL0StateMap<K, N, S> stateMap;

    public ForL0StateMapSnapshot(ForL0StateMap<K, N, S> stateMap) {
        super(stateMap);
        this.stateMap = stateMap;
    }

    @Override
    public Iterator<StateEntry<K, N, S>> getIterator(
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull TypeSerializer<S> stateSerializer,
            @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
        return stateMap.iterator();
    }

    @Override
    public void writeState(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            @Nonnull DataOutputView dov,
            @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer)
            throws IOException {

        // First, collect all valid entries to count them
        java.util.List<StateEntry<K, N, S>> validEntries = new java.util.ArrayList<>();
        final Iterator<StateEntry<K, N, S>> it = stateMap.iterator();

        while (it.hasNext()) {
            StateEntry<K, N, S> e = it.next();
            S value = e.getState();
            if (stateSnapshotTransformer != null) {
                value = stateSnapshotTransformer.filterOrTransform(value);
            }
            if (value != null) {
                // Create a copy of the value to make it effectively final
                final S finalValue = value;
                final StateEntry<K, N, S> originalEntry = e;

                // Create a new StateEntry with the transformed value
                validEntries.add(new StateEntry<K, N, S>() {
                    @Override
                    public K getKey() {
                        return originalEntry.getKey();
                    }

                    @Override
                    public N getNamespace() {
                        return originalEntry.getNamespace();
                    }

                    @Override
                    public S getState() {
                        return finalValue;
                    }
                });
            }
        }

        // Write the number of entries first (required by Flink checkpoint format)
        dov.writeInt(validEntries.size());

        // Then write all entries in the correct order: namespace -> key -> state
        for (StateEntry<K, N, S> entry : validEntries) {
            namespaceSerializer.serialize(entry.getNamespace(), dov);
            keySerializer.serialize(entry.getKey(), dov);
            stateSerializer.serialize(entry.getState(), dov);
        }
    }
}
