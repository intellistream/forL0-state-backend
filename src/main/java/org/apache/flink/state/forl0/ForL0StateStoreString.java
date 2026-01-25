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
 * Specialized StateStore for String keys.
 * 
 * <p>This implementation uses {@link SwissTableString} with strong-typed String[] array,
 * avoiding checkcast overhead and enabling better JIT optimization.
 * Override methods are declared final for JIT optimization.
 * 
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateStoreString<N, S> extends ForL0StateStore<String, N, S> {

    private static final int INITIAL_TABLE_CAPACITY = 64;

    private final SwissTableString<S>[] tablesString;
    private final Map<N, SwissTableString<S>>[] namespaceMapsString;
    private final boolean isVoidNamespaceString;
    private final int keyGroupOffsetString;

    @SuppressWarnings("unchecked")
    public ForL0StateStoreString(
            KeyGroupRange keyGroupRange,
            TypeSerializer<String> keySerializer,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        super(keyGroupRange, keySerializer, metaInfo);
        
        this.keyGroupOffsetString = keyGroupRange.getStartKeyGroup();
        int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        
        this.isVoidNamespaceString = metaInfo.getNamespaceSerializer() instanceof VoidNamespaceSerializer;
        
        if (isVoidNamespaceString) {
            this.tablesString = new SwissTableString[numKeyGroups];
            this.namespaceMapsString = null;
        } else {
            this.tablesString = null;
            this.namespaceMapsString = new HashMap[numKeyGroups];
        }
    }

    @Override
    public final S get(String key, N namespace, int keyGroup) {
        int hash = hashString(key);
        int idx = keyGroup - keyGroupOffsetString;
        SwissTableString<S> table;
        
        if (isVoidNamespaceString) {
            table = tablesString[idx];
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return null;
        }
        return table.get(hash, key);
    }

    @Override
    public final void put(String key, N namespace, S value, int keyGroup) {
        int hash = hashString(key);
        int idx = keyGroup - keyGroupOffsetString;
        SwissTableString<S> table;
        
        if (isVoidNamespaceString) {
            table = tablesString[idx];
            if (table == null) {
                table = new SwissTableString<>(INITIAL_TABLE_CAPACITY);
                tablesString[idx] = table;
            }
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsString[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableString<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, key);
            
            if (result == SwissTableString.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableString.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableString.SLOT_MASK;
            table.values[slot] = value;
            return;
        }
    }

    @Override
    public final S remove(String key, N namespace, int keyGroup) {
        int hash = hashString(key);
        int idx = keyGroup - keyGroupOffsetString;
        
        if (isVoidNamespaceString) {
            SwissTableString<S> table = tablesString[idx];
            if (table == null) {
                return null;
            }
            return table.remove(hash, key);
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            if (nsMap == null) {
                return null;
            }
            SwissTableString<S> table = nsMap.get(namespace);
            if (table == null) {
                return null;
            }
            
            S removed = table.remove(hash, key);
            
            if (table.isEmpty()) {
                nsMap.remove(namespace);
            }
            
            return removed;
        }
    }

    @Override
    public final <T> void transform(
            String key, N namespace, T value, 
            StateTransformationFunction<S, T> transformation,
            int keyGroup) throws Exception {
        int hash = hashString(key);
        int idx = keyGroup - keyGroupOffsetString;
        SwissTableString<S> table;
        
        if (isVoidNamespaceString) {
            table = tablesString[idx];
            if (table == null) {
                table = new SwissTableString<>(INITIAL_TABLE_CAPACITY);
                tablesString[idx] = table;
            }
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            if (nsMap == null) {
                nsMap = new HashMap<>(8);
                namespaceMapsString[idx] = nsMap;
            }
            table = nsMap.get(namespace);
            if (table == null) {
                table = new SwissTableString<>(INITIAL_TABLE_CAPACITY);
                nsMap.put(namespace, table);
            }
        }
        
        while (true) {
            int result = table.put(hash, key);
            
            if (result == SwissTableString.NEED_REHASH) {
                table.rehash();
                continue;
            }
            if (result == SwissTableString.NEED_GROW) {
                table.grow();
                continue;
            }
            
            int slot = result & SwissTableString.SLOT_MASK;
            @SuppressWarnings("unchecked")
            S oldState = (S) table.values[slot];
            table.values[slot] = transformation.apply(oldState, value);
            return;
        }
    }

    @Override
    public final boolean containsKey(String key, N namespace, int keyGroup) {
        int hash = hashString(key);
        int idx = keyGroup - keyGroupOffsetString;
        SwissTableString<S> table;
        
        if (isVoidNamespaceString) {
            table = tablesString[idx];
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            table = (nsMap == null) ? null : nsMap.get(namespace);
        }
        
        if (table == null) {
            return false;
        }
        return table.containsKey(hash, key);
    }

    // String hash: use String.hashCode() + smear
    private static int hashString(String key) {
        int h = key.hashCode();
        return (int) (0x1b873593 * Integer.rotateLeft(h * 0xcc9e2d51, 15));
    }

    @Override
    public Stream<String> getKeys(N namespace) {
        List<String> keys = new ArrayList<>();
        
        if (isVoidNamespaceString) {
            for (SwissTableString<S> table : tablesString) {
                if (table != null) {
                    table.collectKeys(keys);
                }
            }
        } else {
            for (Map<N, SwissTableString<S>> nsMap : namespaceMapsString) {
                if (nsMap != null) {
                    SwissTableString<S> table = nsMap.get(namespace);
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
    public Stream<Tuple2<String, N>> getKeysAndNamespaces() {
        List<Tuple2<String, N>> result = new ArrayList<>();
        
        if (isVoidNamespaceString) {
            N voidNs = (N) VoidNamespace.INSTANCE;
            for (SwissTableString<S> table : tablesString) {
                if (table != null) {
                    for (Iterator<SwissTableString.Entry<S>> it = table.iterator(); it.hasNext(); ) {
                        result.add(Tuple2.of(it.next().getKey(), voidNs));
                    }
                }
            }
        } else {
            for (Map<N, SwissTableString<S>> nsMap : namespaceMapsString) {
                if (nsMap != null) {
                    for (Map.Entry<N, SwissTableString<S>> nsEntry : nsMap.entrySet()) {
                        N namespace = nsEntry.getKey();
                        SwissTableString<S> table = nsEntry.getValue();
                        for (Iterator<SwissTableString.Entry<S>> it = table.iterator(); it.hasNext(); ) {
                            result.add(Tuple2.of(it.next().getKey(), namespace));
                        }
                    }
                }
            }
        }
        return result.stream();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<StateEntry<String, N, S>> entries(int keyGroup) {
        int idx = keyGroup - keyGroupOffsetString;
        
        if (isVoidNamespaceString) {
            SwissTableString<S> table = tablesString[idx];
            if (table == null || table.isEmpty()) {
                return Collections.emptyList();
            }
            
            N voidNs = (N) VoidNamespace.INSTANCE;
            
            return () -> new Iterator<StateEntry<String, N, S>>() {
                private final Iterator<SwissTableString.Entry<S>> inner = table.iterator();
                
                @Override
                public boolean hasNext() {
                    return inner.hasNext();
                }
                
                @Override
                public StateEntry<String, N, S> next() {
                    SwissTableString.Entry<S> e = inner.next();
                    return new SimpleStateEntry<>(e.getKey(), voidNs, e.getState());
                }
            };
        } else {
            Map<N, SwissTableString<S>> nsMap = namespaceMapsString[idx];
            if (nsMap == null || nsMap.isEmpty()) {
                return Collections.emptyList();
            }
            
            return () -> new Iterator<StateEntry<String, N, S>>() {
                private final Iterator<Map.Entry<N, SwissTableString<S>>> nsIter = 
                        nsMap.entrySet().iterator();
                private N currentNamespace;
                private Iterator<SwissTableString.Entry<S>> tableIter;
                
                @Override
                public boolean hasNext() {
                    while ((tableIter == null || !tableIter.hasNext()) && nsIter.hasNext()) {
                        Map.Entry<N, SwissTableString<S>> entry = nsIter.next();
                        currentNamespace = entry.getKey();
                        tableIter = entry.getValue().iterator();
                    }
                    return tableIter != null && tableIter.hasNext();
                }
                
                @Override
                public StateEntry<String, N, S> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    SwissTableString.Entry<S> e = tableIter.next();
                    return new SimpleStateEntry<>(e.getKey(), currentNamespace, e.getState());
                }
            };
        }
    }

    @Override
    public int size() {
        int total = 0;
        
        if (isVoidNamespaceString) {
            for (SwissTableString<S> table : tablesString) {
                if (table != null) {
                    total += table.size();
                }
            }
        } else {
            for (Map<N, SwissTableString<S>> nsMap : namespaceMapsString) {
                if (nsMap != null) {
                    for (SwissTableString<S> table : nsMap.values()) {
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
        if (isVoidNamespaceString) {
            return size();
        }
        
        int count = 0;
        for (Map<N, SwissTableString<S>> nsMap : namespaceMapsString) {
            if (nsMap != null) {
                SwissTableString<S> table = nsMap.get((N) namespace);
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
