package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Specialized StateStore for Long keys.
 * 
 * <p>This implementation uses {@link SwissTableLong} for zero Pointer Chasing key comparison.
 * Override methods are declared final for JIT optimization.
 * 
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreLong<N, S> extends ForL0StateStore<Long, N, S> {

    /** Initial capacity for each SwissTableLong. */
    private static final int INITIAL_TABLE_CAPACITY = 64;

    /** VoidNamespace mode: direct SwissTableLong array. */
    private final SwissTableLong<S>[] tablesLong;

    /** General Namespace mode: HashMap per key group. */
    private final Map<N, SwissTableLong<S>>[] namespaceMapsLong;

    /** Whether this store uses VoidNamespace mode. */
    private final boolean isVoidNamespaceLong;

    /** Offset for translating key group to array index. */
    private final int keyGroupOffsetLong;

    @SuppressWarnings("unchecked")
    public ForL0StateStoreLong(
            KeyGroupRange keyGroupRange,
            TypeSerializer<Long> keySerializer,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        super(keyGroupRange, keySerializer, metaInfo);
        
        this.keyGroupOffsetLong = keyGroupRange.getStartKeyGroup();
        int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        
        this.isVoidNamespaceLong = metaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer;
        
        if (isVoidNamespaceLong) {
            this.tablesLong = new SwissTableLong[numKeyGroups];
            this.namespaceMapsLong = null;
        } else {
            this.tablesLong = null;
            this.namespaceMapsLong = new HashMap[numKeyGroups];
        }
    }

    // ========== Core Operations (final for JIT optimization) ==========

    @Override
    public final S get(Long key, N namespace, int keyGroup) {
        long k = key.longValue();  // One unboxing
        int hash = hashLong(k);
        int idx = keyGroup - keyGroupOffsetLong;
        SwissTableLong<S> table;
        
        if (isVoidNamespaceLong) {
            table = tablesLong[idx];
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return null;
        }
        return table.get(hash, k);
    }

    @Override
    public final void put(Long key, N namespace, S value, int keyGroup) {
        long k = key.longValue();
        int hash = hashLong(k);
        int idx = keyGroup - keyGroupOffsetLong;
        SwissTableLong<S> table;
        
        if (isVoidNamespaceLong) {
            table = tablesLong[idx];
            if (table == null) {
                table = new SwissTableLong<>(INITIAL_TABLE_CAPACITY);
                tablesLong[idx] = table;
            }
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsLong[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableLong<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, k);
            
            if (result == SwissTableLong.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableLong.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableLong.SLOT_MASK;
            table.values[slot] = value;
            return;
        }
    }

    @Override
    public final S remove(Long key, N namespace, int keyGroup) {
        long k = key.longValue();
        int hash = hashLong(k);
        int idx = keyGroup - keyGroupOffsetLong;
        
        if (isVoidNamespaceLong) {
            SwissTableLong<S> table = tablesLong[idx];
            if (table == null) {
                return null;
            }
            return table.remove(hash, k);
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null) {
                return null;
            }
            SwissTableLong<S> table = nsMap.get(namespace);
            if (table == null) {
                return null;
            }
            
            S removed = table.remove(hash, k);
            
            if (table.isEmpty()) {
                nsMap.remove(namespace);
            }
            
            return removed;
        }
    }

    @Override
    public final <T> void transform(
            Long key, N namespace, T value, 
            StateTransformationFunction<S, T> transformation,
            int keyGroup) throws Exception {
        long k = key.longValue();
        int hash = hashLong(k);
        int idx = keyGroup - keyGroupOffsetLong;
        SwissTableLong<S> table;
        
        if (isVoidNamespaceLong) {
            table = tablesLong[idx];
            if (table == null) {
                table = new SwissTableLong<>(INITIAL_TABLE_CAPACITY);
                tablesLong[idx] = table;
            }
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsLong[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableLong<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, k);
            
            if (result == SwissTableLong.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableLong.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableLong.SLOT_MASK;
            @SuppressWarnings("unchecked")
            S oldState = (S) table.values[slot];
            table.values[slot] = transformation.apply(oldState, value);
            return;
        }
    }

    @Override
    public final boolean containsKey(Long key, N namespace, int keyGroup) {
        long k = key.longValue();
        int hash = hashLong(k);
        int idx = keyGroup - keyGroupOffsetLong;
        SwissTableLong<S> table;
        
        if (isVoidNamespaceLong) {
            table = tablesLong[idx];
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return false;
        }
        return table.containsKey(hash, k);
    }

    // ========== Hash function (inlined XOR folding + smear) ==========

    private static int hashLong(long key) {
        int h = (int) (key ^ (key >>> 32));
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }

    // ========== Iteration ==========

    @Override
    public Stream<Long> getKeys(N namespace) {
        List<Long> keys = new ArrayList<>();
        
        if (isVoidNamespaceLong) {
            for (SwissTableLong<S> table : tablesLong) {
                if (table != null) {
                    table.collectKeys(keys);
                }
            }
        } else {
            for (Map<N, SwissTableLong<S>> nsMap : namespaceMapsLong) {
                if (nsMap != null) {
                    SwissTableLong<S> table = nsMap.get(namespace);
                    if (table != null) {
                        table.collectKeys(keys);
                    }
                }
            }
        }
        return keys.stream();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<Tuple2<Long, N>> getKeysAndNamespaces() {
        List<Tuple2<Long, N>> result = new ArrayList<>();
        
        if (isVoidNamespaceLong) {
            N voidNs = (N) VoidNamespace.INSTANCE;
            for (SwissTableLong<S> table : tablesLong) {
                if (table != null) {
                    for (Iterator<SwissTableLong.Entry<S>> it = table.iterator(); it.hasNext(); ) {
                        result.add(Tuple2.of(it.next().getKeyBoxed(), voidNs));
                    }
                }
            }
        } else {
            for (Map<N, SwissTableLong<S>> nsMap : namespaceMapsLong) {
                if (nsMap != null) {
                    for (Map.Entry<N, SwissTableLong<S>> nsEntry : nsMap.entrySet()) {
                        N namespace = nsEntry.getKey();
                        SwissTableLong<S> table = nsEntry.getValue();
                        for (Iterator<SwissTableLong.Entry<S>> it = table.iterator(); it.hasNext(); ) {
                            result.add(Tuple2.of(it.next().getKeyBoxed(), namespace));
                        }
                    }
                }
            }
        }
        return result.stream();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<StateEntry<Long, N, S>> entries(int keyGroup) {
        int idx = keyGroup - keyGroupOffsetLong;
        
        if (isVoidNamespaceLong) {
            SwissTableLong<S> table = tablesLong[idx];
            if (table == null || table.isEmpty()) {
                return Collections.emptyList();
            }
            
            N voidNs = (N) VoidNamespace.INSTANCE;
            
            return () -> new Iterator<StateEntry<Long, N, S>>() {
                private final Iterator<SwissTableLong.Entry<S>> inner = table.iterator();
                
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }
                
                @Override
                public StateEntry<Long, N, S> next() {
                    SwissTableLong.Entry<S> e = inner.next();
                    return new SimpleStateEntry<>(e.getKeyBoxed(), voidNs, e.getState());
                }
            };
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null || nsMap.isEmpty()) {
                return Collections.emptyList();
            }
            
            return () -> new Iterator<StateEntry<Long, N, S>>() {
                private final Iterator<Map.Entry<N, SwissTableLong<S>>> nsIter = 
                        nsMap.entrySet().iterator();
                private N currentNamespace;
                private Iterator<SwissTableLong.Entry<S>> tableIter;
                
                @Override
                public boolean hasNext() {
                    while ((tableIter == null || !tableIter.hasNext()) && nsIter.hasNext()) {
                        Map.Entry<N, SwissTableLong<S>> entry = nsIter.next();
                        currentNamespace = entry.getKey();
                        tableIter = entry.getValue().iterator();
                    }
                    return tableIter != null && tableIter.hasNext();
                }
                
                @Override
                public StateEntry<Long, N, S> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    SwissTableLong.Entry<S> e = tableIter.next();
                    return new SimpleStateEntry<>(e.getKeyBoxed(), currentNamespace, e.getState());
                }
            };
        }
    }

    @Override
    public int size() {
        int total = 0;
        
        if (isVoidNamespaceLong) {
            for (SwissTableLong<S> table : tablesLong) {
                if (table != null) {
                    total += table.size();
                }
            }
        } else {
            for (Map<N, SwissTableLong<S>> nsMap : namespaceMapsLong) {
                if (nsMap != null) {
                    for (SwissTableLong<S> table : nsMap.values()) {
                        total += table.size();
                    }
                }
            }
        }
        return total;
    }

    @Override
    public final int getEntryCount(int keyGroup) {
        int idx = keyGroup - keyGroupOffsetLong;
        
        if (isVoidNamespaceLong) {
            SwissTableLong<S> table = tablesLong[idx];
            return (table == null) ? 0 : table.size();
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null) {
                return 0;
            }
            int count = 0;
            for (SwissTableLong<S> table : nsMap.values()) {
                count += table.size();
            }
            return count;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void forEachInKeyGroup(int keyGroup, StateEntryConsumer<Long, N, S> consumer) throws IOException {
        int idx = keyGroup - keyGroupOffsetLong;
        
        if (isVoidNamespaceLong) {
            SwissTableLong<S> table = tablesLong[idx];
            if (table == null || table.isEmpty()) {
                return;
            }
            N voidNs = (N) VoidNamespace.INSTANCE;
            try {
                table.<IOException>forEachEntry((key, value) -> consumer.accept(key, voidNs, value));
            } catch (Exception e) {
                throwAsIOException(e);
            }
        } else {
            Map<N, SwissTableLong<S>> nsMap = namespaceMapsLong[idx];
            if (nsMap == null || nsMap.isEmpty()) {
                return;
            }
            for (Map.Entry<N, SwissTableLong<S>> nsEntry : nsMap.entrySet()) {
                N namespace = nsEntry.getKey();
                SwissTableLong<S> table = nsEntry.getValue();
                try {
                    table.<IOException>forEachEntry((key, value) -> consumer.accept(key, namespace, value));
                } catch (Exception e) {
                    throwAsIOException(e);
                }
            }
        }
    }
    
    private static void throwAsIOException(Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        throw new IOException(e);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int sizeOfNamespace(Object namespace) {
        if (isVoidNamespaceLong) {
            return size();
        }
        
        int count = 0;
        for (Map<N, SwissTableLong<S>> nsMap : namespaceMapsLong) {
            if (nsMap != null) {
                SwissTableLong<S> table = nsMap.get((N) namespace);
                if (table != null) {
                    count += table.size();
                }
            }
        }
        return count;
    }

    /**
     * Simple StateEntry implementation.
     */
    private static class SimpleStateEntry<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S state;

        SimpleStateEntry(K key, N namespace, S state) {
            this.key = key;
            this.namespace = namespace;
            this.state = state;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public N getNamespace() {
            return namespace;
        }

        @Override
        public S getState() {
            return state;
        }
    }
}
