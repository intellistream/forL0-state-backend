package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;
    private final MemoryManagerAllocator allocator;
    private final ForL0BucketTable table;
    private int size = 0;

    public ForL0StateMap(int initPow2,
                         TypeSerializer<K> keySerializer,
                         TypeSerializer<N> namespaceSerializer,
                         TypeSerializer<S> stateSerializer,
                         MemoryManagerAllocator allocator) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.stateSerializer = stateSerializer;
        this.allocator = allocator;
        this.table = new ForL0BucketTable(initPow2, allocator);
    }

    @Override
    public void close() throws Exception {
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
        int hash = jenkinsHash(kb, nb);
        short tag = (short) (murmur16(kb) & 0xFFFF);
        long entry = table.lookup(kb, nb, hash, tag);
//        LOG.info("Getting state for key {} namespace {}, entry: {}",
//                key, namespace, entry == 0 ? null : deserializeState(entry));
        return entry == 0 ? null : deserializeState(entry);
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
        short tag = (short) (murmur16(kb) & 0xFFFF);
        long entry = table.lookup(kb, nb, hash, tag);
        if(entry != 0) {
            overwriteState(entry, vb);
            return;
        }
        long newPtr = createEntry(hash, kb, nb, vb);
        table.insert(hash, tag, newPtr);
        size++;
     }

    @Override
    public S putAndGetOld(K key, N namespace, S state) {
        return null;
    }

    @Override
    public void remove(K key, N namespace) {

    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        return null;
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

    // Serialization

    @SuppressWarnings("unchecked")
    private byte[] serialize(TypeSerializer<?> serializer, Object obj) {
        if (obj == null) {
            return new byte[0];
        }
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            ((TypeSerializer)serializer).serialize(obj, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.getCopyOfBuffer();
    }


    private S deserializeState(long addr) {
        int k = ForL0EntryAccess.kl(addr);
        int n = ForL0EntryAccess.nl(addr);
        int v = ForL0EntryAccess.vl(addr);
        long p = addr + ForL0EntryAccess.HEADER + k + n;
        byte[] buf = new byte[v];
        Unsafe U = UnsafeUtils.unsafe();
        long ba = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        for (int i = 0; i < v; i++) {
            buf[i] = U.getByte(p + i);
        }
        DataInputDeserializer in = new DataInputDeserializer(buf);
        try {
            return stateSerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Create/Overwrite Entry

    private long createEntry(int hash, byte[] k, byte[] n, byte[] v) {
        int len = ForL0EntryAccess.HEADER + k.length + n.length + v.length;
        List<MemorySegment> pages = null;
        try {
            pages = allocator.allocate(len);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
        assert pages.size() == 1 : "entry allocation expects a single contiguous MemorySegment; adjust segment-size if necessary";
        MemorySegment seg = pages.get(0);
        long addr = seg.getAddress();
        ForL0EntryAccess.hash(addr, hash);
        Unsafe U = UnsafeUtils.unsafe();

        U.putInt(addr + ForL0EntryAccess.KL, k.length);
        U.putInt(addr + ForL0EntryAccess.NL, n.length);
        U.putInt(addr + ForL0EntryAccess.VL, v.length);
        ForL0EntryAccess.next(addr, 0);

        long p = addr + ForL0EntryAccess.HEADER;
        long ba = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        U.copyMemory(k, ba, null, p, k.length);
        p += k.length;
        U.copyMemory(n, ba, null, p, n.length);
        p += n.length;
        U.copyMemory(v, ba, null, p, v.length);
        return addr;
    }

    private void overwriteState(long addr, byte[] v) {
        int oldLen = ForL0EntryAccess.vl(addr);
        if (v.length <= oldLen) {
            long p = addr + ForL0EntryAccess.HEADER + ForL0EntryAccess.kl(addr) + ForL0EntryAccess.nl(addr);
            Unsafe U = UnsafeUtils.unsafe();
            long ba = Unsafe.ARRAY_BYTE_BASE_OFFSET;
            U.copyMemory(v, ba, null, p, v.length);
            U.putInt(addr + ForL0EntryAccess.VL, v.length);
        } else {
            // To Be Implemented
            LOG.info("Overwriting existing entry at {}, but not implemented when new is bigger than old", addr);
            // System.out.println("Not Implemented");
        }
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
