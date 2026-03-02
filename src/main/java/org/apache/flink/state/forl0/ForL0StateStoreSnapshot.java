package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Snapshot of a {@link ForL0StateStore} for checkpointing.
 * 
 * <p>This implements {@link IterableStateSnapshot} to work with Flink's
 * standard checkpoint writers like {@link org.apache.flink.runtime.state.FullSnapshotAsyncWriter}.
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreSnapshot<K, N, S> implements IterableStateSnapshot<K, N, S> {

    /** The source store (for snapshot-on-write, we just reference the live data). */
    private final ForL0StateStore<K, N, S> store;

    /** Snapshot of the meta info. */
    private final StateMetaInfoSnapshot metaInfoSnapshot;

    public ForL0StateStoreSnapshot(ForL0StateStore<K, N, S> store) {
        this.store = store;
        this.metaInfoSnapshot = store.getMetaInfo().snapshot();
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot getMetaInfoSnapshot() {
        return metaInfoSnapshot;
    }

    @Nonnull
    @Override
    public StateKeyGroupWriter getKeyGroupWriter() {
        return new ForL0StateKeyGroupWriter();
    }

    @Override
    public void release() {
        // No resources to release - we don't make a copy
    }

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> getIterator(int keyGroupId) {
        Iterable<StateEntry<K, N, S>> entries = store.entries(keyGroupId);
        if (entries == null) {
            return Collections.emptyIterator();
        }
        return entries.iterator();
    }

    /**
     * Gets the key serializer.
     */
    public TypeSerializer<K> getKeySerializer() {
        return store.getKeySerializer();
    }

    /**
     * Gets the namespace serializer.
     */
    public TypeSerializer<N> getNamespaceSerializer() {
        return store.getNamespaceSerializer();
    }

    /**
     * Gets the state serializer.
     */
    public TypeSerializer<S> getStateSerializer() {
        return store.getStateSerializer();
    }

    /**
     * Gets the state name.
     */
    public String getStateName() {
        return store.getStateName();
    }

    /**
     * Writer for key-group data.
     * <p>Uses Flink's standard checkpoint format: namespace -> key -> state order.
     * <p>Optimized: single-pass iteration using forEachInKeyGroup (zero object allocation),
     * entry count from O(1) SwissTable.size() instead of counting pass.
     */
    private class ForL0StateKeyGroupWriter implements StateKeyGroupWriter {
        @Override
        public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId)
                throws IOException {
            // O(1) entry count — no counting pass needed
            int count = store.getEntryCount(keyGroupId);
            dov.writeInt(count);
            
            if (count == 0) {
                return;
            }
            
            // Single-pass zero-allocation iteration
            final TypeSerializer<N> nsSerializer = store.getNamespaceSerializer();
            final TypeSerializer<K> kSerializer = store.getKeySerializer();
            final TypeSerializer<S> sSerializer = store.getStateSerializer();
            
            store.forEachInKeyGroup(keyGroupId, (key, namespace, state) -> {
                nsSerializer.serialize(namespace, dov);
                kSerializer.serialize(key, dov);
                sSerializer.serialize(state, dov);
            });
        }
    }
}
