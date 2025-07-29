package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;

    private int size = 0;

    public ForL0StateMap(int initPow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        return (S) null; // Placeholder for actual implementation
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        return get(key, namespace) != null;
    }

    @Override
    public void put(K key, N namespace, S state) {
     }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        return (S) null; // Placeholder for actual implementation
    }

    @Override
    public void remove(K key, N namespace) {
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        return (S) null; // Placeholder for actual implementation
    }

    @Override
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {

    }

    @Override
    public Stream<K> getKeys(N namespace) {
        return Stream.empty();
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        return 0;
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return null;
    }

}
