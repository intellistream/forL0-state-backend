package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Specialized StateStore for Integer keys.
 * 
 * <p>This implementation uses {@link SwissTableInt} for zero Pointer Chasing key comparison.
 * Override methods are declared final for JIT optimization.
 * 
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreInt<N, S> extends ForL0StateStore<Integer, N, S> {

    private static final int INITIAL_TABLE_CAPACITY = 64;

    private final SwissTableInt<S>[] tablesInt;
    private final Map<N, SwissTableInt<S>>[] namespaceMapsInt;
    private final boolean isVoidNamespaceInt;
    private final int keyGroupOffsetInt;

    @SuppressWarnings("unchecked")
    public ForL0StateStoreInt(
            KeyGroupRange keyGroupRange,
            TypeSerializer<Integer> keySerializer,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        super(keyGroupRange, keySerializer, metaInfo);
        
        this.keyGroupOffsetInt = keyGroupRange.getStartKeyGroup();
        int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        
        this.isVoidNamespaceInt = metaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer;
        
        if (isVoidNamespaceInt) {
            this.tablesInt = new SwissTableInt[numKeyGroups];
            this.namespaceMapsInt = null;
        } else {
            this.tablesInt = null;
            this.namespaceMapsInt = new HashMap[numKeyGroups];
        }
    }

    @Override
    public final S get(Integer key, N namespace, int keyGroup) {
        int k = key.intValue();
        int hash = hashInt(k);
        int idx = keyGroup - keyGroupOffsetInt;
        SwissTableInt<S> table;
        
        if (isVoidNamespaceInt) {
            table = tablesInt[idx];
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return null;
        }
        return table.get(hash, k);
    }

    @Override
    public final void put(Integer key, N namespace, S value, int keyGroup) {
        int k = key.intValue();
        int hash = hashInt(k);
        int idx = keyGroup - keyGroupOffsetInt;
        SwissTableInt<S> table;
        
        if (isVoidNamespaceInt) {
            table = tablesInt[idx];
            if (table == null) {
                table = new SwissTableInt<>(INITIAL_TABLE_CAPACITY);
                tablesInt[idx] = table;
            }
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsInt[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableInt<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, k);
            
            if (result == SwissTableInt.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableInt.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableInt.SLOT_MASK;
            table.values[slot] = value;
            return;
        }
    }

    @Override
    public final S remove(Integer key, N namespace, int keyGroup) {
        int k = key.intValue();
        int hash = hashInt(k);
        int idx = keyGroup - keyGroupOffsetInt;
        
        if (isVoidNamespaceInt) {
            SwissTableInt<S> table = tablesInt[idx];
            if (table == null) {
                return null;
            }
            return table.remove(hash, k);
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            if (nsMap == null) {
                return null;
            }
            SwissTableInt<S> table = nsMap.get(namespace);
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
            Integer key, N namespace, T value, 
            StateTransformationFunction<S, T> transformation,
            int keyGroup) throws Exception {
        int k = key.intValue();
        int hash = hashInt(k);
        int idx = keyGroup - keyGroupOffsetInt;
        SwissTableInt<S> table;
        
        if (isVoidNamespaceInt) {
            table = tablesInt[idx];
            if (table == null) {
                table = new SwissTableInt<>(INITIAL_TABLE_CAPACITY);
                tablesInt[idx] = table;
            }
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsInt[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableInt<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, k);
            
            if (result == SwissTableInt.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableInt.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableInt.SLOT_MASK;
            @SuppressWarnings("unchecked")
            S oldState = (S) table.values[slot];
            table.values[slot] = transformation.apply(oldState, value);
            return;
        }
    }

    @Override
    public final boolean containsKey(Integer key, N namespace, int keyGroup) {
        int k = key.intValue();
        int hash = hashInt(k);
        int idx = keyGroup - keyGroupOffsetInt;
        SwissTableInt<S> table;
        
        if (isVoidNamespaceInt) {
            table = tablesInt[idx];
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return false;
        }
        return table.containsKey(hash, k);
    }

    // Integer.hashCode() returns the value itself, so just use smear directly
    private static int hashInt(int key) {
        return (int) (0x1b873593 * Integer.rotateLeft(key * 0xcc9e2d51, 15));
    }

    @Override
    public Stream<Integer> getKeys(N namespace) {
        List<Integer> keys = new ArrayList<>();
        
        if (isVoidNamespaceInt) {
            for (SwissTableInt<S> table : tablesInt) {
                if (table != null) {
                    table.collectKeys(keys);
                }
            }
        } else {
            for (Map<N, SwissTableInt<S>> nsMap : namespaceMapsInt) {
                if (nsMap != null) {
                    SwissTableInt<S> table = nsMap.get(namespace);
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
    public Stream<Tuple2<Integer, N>> getKeysAndNamespaces() {
        List<Tuple2<Integer, N>> result = new ArrayList<>();
        
        if (isVoidNamespaceInt) {
            N voidNs = (N) VoidNamespace.INSTANCE;
            for (SwissTableInt<S> table : tablesInt) {
                if (table != null) {
                    for (Iterator<SwissTableInt.Entry<S>> it = table.iterator(); it.hasNext(); ) {
                        result.add(Tuple2.of(it.next().getKeyBoxed(), voidNs));
                    }
                }
            }
        } else {
            for (Map<N, SwissTableInt<S>> nsMap : namespaceMapsInt) {
                if (nsMap != null) {
                    for (Map.Entry<N, SwissTableInt<S>> nsEntry : nsMap.entrySet()) {
                        N namespace = nsEntry.getKey();
                        SwissTableInt<S> table = nsEntry.getValue();
                        for (Iterator<SwissTableInt.Entry<S>> it = table.iterator(); it.hasNext(); ) {
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
    public Iterable<StateEntry<Integer, N, S>> entries(int keyGroup) {
        int idx = keyGroup - keyGroupOffsetInt;
        
        if (isVoidNamespaceInt) {
            SwissTableInt<S> table = tablesInt[idx];
            if (table == null || table.isEmpty()) {
                return Collections.emptyList();
            }
            
            N voidNs = (N) VoidNamespace.INSTANCE;
            
            return () -> new Iterator<StateEntry<Integer, N, S>>() {
                private final Iterator<SwissTableInt.Entry<S>> inner = table.iterator();
                
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }
                
                @Override
                public StateEntry<Integer, N, S> next() {
                    SwissTableInt.Entry<S> e = inner.next();
                    return new SimpleStateEntry<>(e.getKeyBoxed(), voidNs, e.getState());
                }
            };
        } else {
            Map<N, SwissTableInt<S>> nsMap = namespaceMapsInt[idx];
            if (nsMap == null || nsMap.isEmpty()) {
                return Collections.emptyList();
            }
            
            return () -> new Iterator<StateEntry<Integer, N, S>>() {
                private final Iterator<Map.Entry<N, SwissTableInt<S>>> nsIter = 
                        nsMap.entrySet().iterator();
                private N currentNamespace;
                private Iterator<SwissTableInt.Entry<S>> tableIter;
                
                @Override
                public boolean hasNext() {
                    while ((tableIter == null || !tableIter.hasNext()) && nsIter.hasNext()) {
                        Map.Entry<N, SwissTableInt<S>> entry = nsIter.next();
                        currentNamespace = entry.getKey();
                        tableIter = entry.getValue().iterator();
                    }
                    return tableIter != null && tableIter.hasNext();
                }
                
                @Override
                public StateEntry<Integer, N, S> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    SwissTableInt.Entry<S> e = tableIter.next();
                    return new SimpleStateEntry<>(e.getKeyBoxed(), currentNamespace, e.getState());
                }
            };
        }
    }

    @Override
    public int size() {
        int total = 0;
        
        if (isVoidNamespaceInt) {
            for (SwissTableInt<S> table : tablesInt) {
                if (table != null) {
                    total += table.size();
                }
            }
        } else {
            for (Map<N, SwissTableInt<S>> nsMap : namespaceMapsInt) {
                if (nsMap != null) {
                    for (SwissTableInt<S> table : nsMap.values()) {
                        total += table.size();
                    }
                }
            }
        }
        return total;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int sizeOfNamespace(Object namespace) {
        if (isVoidNamespaceInt) {
            return size();
        }
        
        int count = 0;
        for (Map<N, SwissTableInt<S>> nsMap : namespaceMapsInt) {
            if (nsMap != null) {
                SwissTableInt<S> table = nsMap.get((N) namespace);
                if (table != null) {
                    count += table.size();
                }
            }
        }
        return count;
    }

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
        public K getKey() { return key; }

        @Override
        public N getNamespace() { return namespace; }

        @Override
        public S getState() { return state; }
    }
}
