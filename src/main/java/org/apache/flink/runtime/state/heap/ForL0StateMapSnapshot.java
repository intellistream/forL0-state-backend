package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Snapshot implementation for {@link ForL0StateMap}.
 * 
 * <p>This is a synchronous snapshot implementation optimized for performance.
 * Unlike {@link CopyOnWriteStateMapSnapshot}, it does not support async snapshots
 * and does not require copy-on-write semantics.
 * 
 * <p>Performance characteristics:
 * <ul>
 *   <li>No transformer: Single-pass direct serialization (optimal)</li>
 *   <li>With transformer: Uses temporary buffer to avoid double traversal</li>
 *   <li>Zero intermediate object allocation in the common case</li>
 * </ul>
 */
public class ForL0StateMapSnapshot<K, N, S>
        extends StateMapSnapshot<K, N, S, ForL0StateMap<K, N, S>> {

    /** Initial buffer size for transformed snapshot (64KB). */
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;

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

        if (stateSnapshotTransformer == null) {
            // Fast path: no transformer, single-pass direct write
            writeStateDirectly(keySerializer, namespaceSerializer, stateSerializer, dov);
        } else {
            // With transformer: use buffer to avoid double traversal
            writeStateWithTransformer(keySerializer, namespaceSerializer, stateSerializer, 
                                      dov, stateSnapshotTransformer);
        }
    }

    /**
     * Fast path: directly serialize all entries in a single pass.
     * No intermediate allocations, maximum performance.
     */
    private void writeStateDirectly(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            DataOutputView dov) throws IOException {
        
        // Write size first (we know it without traversing)
        dov.writeInt(stateMap.size());
        
        // Direct single-pass serialization
        for (StateEntry<K, N, S> entry : stateMap) {
            namespaceSerializer.serialize(entry.getNamespace(), dov);
            keySerializer.serialize(entry.getKey(), dov);
            stateSerializer.serialize(entry.getState(), dov);
        }
    }

    /**
     * Write state with transformer using temporary buffer.
     * This avoids double traversal by buffering serialized data first,
     * then writing count + buffer content.
     */
    private void writeStateWithTransformer(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            DataOutputView dov,
            StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {
        
        // Use temporary buffer to serialize entries
        DataOutputSerializer tempBuffer = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
        int count = 0;
        
        for (StateEntry<K, N, S> entry : stateMap) {
            S transformedValue = stateSnapshotTransformer.filterOrTransform(entry.getState());
            if (transformedValue != null) {
                namespaceSerializer.serialize(entry.getNamespace(), tempBuffer);
                keySerializer.serialize(entry.getKey(), tempBuffer);
                stateSerializer.serialize(transformedValue, tempBuffer);
                count++;
            }
        }
        
        // Write count first, then buffer content
        dov.writeInt(count);
        dov.write(tempBuffer.getSharedBuffer(), 0, tempBuffer.length());
    }
}
