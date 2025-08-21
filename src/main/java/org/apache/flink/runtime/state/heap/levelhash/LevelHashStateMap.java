package org.apache.flink.runtime.state.heap.levelhash;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.ForL0StateMapSnapshot;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class LevelHashStateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LevelHashStateMap.class);

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;

    private final MemoryManagerAllocator allocator;
    private final LevelHashIndex index;
    private final LevelHashEntryArena arena;

    private int size = 0;

    public LevelHashStateMap(int initPow2,
                             TypeSerializer<K> keySerializer,
                             TypeSerializer<N> namespaceSerializer,
                             TypeSerializer<S> stateSerializer,
                             MemoryManagerAllocator allocator) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
        this.allocator = allocator;
        this.index = new LevelHashIndex(allocator, initPow2);
        this.arena = new LevelHashEntryArena(allocator);
    }

    @Override
    public void close() throws Exception {
        arena.close();
        index.close();
        allocator.close();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        byte[] kb = serialize(keySerializer, key);
        byte[] nb = serialize(namespaceSerializer, namespace);
        int hash  = jenkinsHash(kb, nb);

        long ptr = index.get(hash);
        if (ptr == 0) {
            return null;
        }
        byte[] vb = arena.getValue(ptr);
//        LOG.info("Getting state for key {} namespace {}, entry: {}",
//                key, namespace, entry == 0 ? null : deserializeState(entry));
        return deserializeValue(vb);
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        return get(key, namespace) != null;
    }

    @Override
    public void put(K key, N namespace, S state) {
//        LOG.info("Putting state for key {} namespace {}", key, namespace);
        byte[] kb = serialize(keySerializer, key);
        byte[] nb = serialize(namespaceSerializer, namespace);
        byte[] vb = serialize(stateSerializer, state);
        int hash = jenkinsHash(kb, nb);

        long oldPtr = index.get(hash);
        long newPtr;
        try {
            newPtr = arena.put(concat(kb, nb), vb);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
        try {
            index.put(hash, newPtr);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
        if (oldPtr == 0) {
            size++;
        }
     }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        S old = get(key, namespace);
        put(key, namespace, state);
        return old;
    }

    @Override
    public void remove(K key, N namespace) {
        byte[] kb = serialize(stateSerializer, key);
        byte[] nb = serialize(namespaceSerializer, namespace);
        int hash  = jenkinsHash(kb, nb);

        long removed = index.remove(hash);
        if (removed != 0) {
            size--;
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        S old = get(key, namespace);
        remove(key, namespace);
        return old;
    }


    // Not implemented methods

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

    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return null;
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        return 0;
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return null;
    }

    // ---------------------------------------------------------------------
    //  Serialization helpers
    // ---------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private byte[] serialize(TypeSerializer<?> ser, Object obj) {
        if (obj == null) { return new byte[0]; }
        org.apache.flink.core.memory.DataOutputSerializer out =
                new org.apache.flink.core.memory.DataOutputSerializer(64);
        try {
            ((TypeSerializer) ser).serialize(obj, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.getCopyOfBuffer();
    }

    private S deserializeValue(byte[] buf) {
        org.apache.flink.core.memory.DataInputDeserializer in =
                new org.apache.flink.core.memory.DataInputDeserializer(buf);
        try {
            return stateSerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }


    // Hash/Tag

    private static int jenkinsHash(byte[] k, byte[] n) {
        int hash = 0;
        for (byte b : k) hash += b;
        for (byte b : n) hash += b;
        hash += (hash << 10);
        hash ^= (hash >>> 6);
        hash += (hash << 3);
        hash ^= (hash >>> 11);
        hash += (hash << 15);
        return hash;
    }

    private static int murmur16(byte[] key) {
        int hash = 0;
        for (byte b : key) {
            hash ^= b;
            hash *= 0x5bd1e995;
            hash ^= hash >>> 15;
        }
        return hash & 0xFFFF;
    }


}
