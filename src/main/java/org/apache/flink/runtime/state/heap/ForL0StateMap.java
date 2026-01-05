package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * ForL0StateMap implementation using Swiss Tables architecture.
 * 
 * <p>This implementation uses a directory-based Swiss Table structure for
 * high-performance key-value state storage with incremental expansion.
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // Core storage using Swiss Tables
    private final SwissMap<K, N, S> map;

    /**
     * The last namespace that was actually inserted. This is a small optimization to reduce
     * duplicate namespace objects (same as CopyOnWriteStateMap).
     */
    private N lastNamespace;

    /**
     * Creates a new ForL0StateMap with Swiss Tables architecture.
     */
    public ForL0StateMap() {
        this.map = new SwissMap<>();
        LOG.debug("[ForL0] StateMap initialized with Swiss Tables architecture");
    }

    @Override
    public void close() throws Exception {
        LOG.info("ForL0StateMap closing - size: {}", map.size());
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public S get(K key, N namespace) {
        return map.get(key, namespace);
    }

    @Override
    public boolean containsKey(K key, N namespace) {
        return get(key, namespace) != null;
    }

    @Override
    public void put(K key, N namespace, S state) {
        // Deduplicate namespace
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        int ptr = map.put(key, namespace);
        map.states[ptr - 1] = state;
    }

    @Override
    @SuppressWarnings("unchecked")
    public S putAndGetOld(K key, N namespace, S state) {
        // Deduplicate namespace
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        int ptr = map.put(key, namespace);
        S oldValue = (S) map.states[ptr - 1];
        map.states[ptr - 1] = state;
        return oldValue;
    }

    @Override
    public void remove(K key, N namespace) {
        map.remove(key, namespace);
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        return map.remove(key, namespace);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        // Deduplicate namespace
        if (namespace.equals(lastNamespace)) {
            namespace = lastNamespace;
        } else {
            lastNamespace = namespace;
        }
        int ptr = map.put(key, namespace);
        S oldState = (S) map.states[ptr - 1];
        map.states[ptr - 1] = transformation.apply(oldState, value);
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        int[] cnt = new int[1];
        map.forEach((k, n, s) -> {
            if (namespace != null && namespace.equals(n)) {
                cnt[0]++;
            }
        });
        return cnt[0];
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        final int batchSize = Math.max(1, recommendedMaxNumberOfReturnedRecords);
        final Iterator<StateEntry<K, N, S>> iter = iterator();
        
        return new InternalKvState.StateIncrementalVisitor<K, N, S>() {
            @Override
            public java.util.List<StateEntry<K, N, S>> nextEntries() {
                java.util.ArrayList<StateEntry<K, N, S>> batch = new java.util.ArrayList<>(batchSize);
                int i = 0;
                while (i < batchSize && iter.hasNext()) {
                    StateEntry<K, N, S> entry = iter.next();
                    batch.add(new StateEntryImpl<>(entry.getKey(), entry.getNamespace(), entry.getState()));
                    i++;
                }
                return batch;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public void update(StateEntry<K, N, S> entry, S newState) {
                if (entry != null) {
                    put(entry.getKey(), entry.getNamespace(), newState);
                }
            }

            @Override
            public void remove(StateEntry<K, N, S> entry) {
                if (entry != null) {
                    ForL0StateMap.this.remove(entry.getKey(), entry.getNamespace());
                }
            }
        };
    }

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return new SwissMapStateEntryIterator();
    }

    /**
     * Lightweight StateEntry implementation for batch operations.
     */
    private static class StateEntryImpl<K, N, S> implements StateEntry<K, N, S> {
        private final K key;
        private final N namespace;
        private final S state;

        StateEntryImpl(K key, N namespace, S state) {
            this.key = key;
            this.namespace = namespace;
            this.state = state;
        }

        @Override public K getKey() { return key; }
        @Override public N getNamespace() { return namespace; }
        @Override public S getState() { return state; }
    }

    /**
     * Iterator that wraps SwissMap's iterator to provide StateEntry interface.
     */
    private class SwissMapStateEntryIterator implements Iterator<StateEntry<K, N, S>> {
        private final Iterator<SwissMap.SwissMapEntry<K, N, S>> inner;
        private final ReusableEntry reusable = new ReusableEntry();

        SwissMapStateEntryIterator() {
            this.inner = map.iterator();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public StateEntry<K, N, S> next() {
            if (!inner.hasNext()) {
                throw new NoSuchElementException();
            }
            SwissMap.SwissMapEntry<K, N, S> entry = inner.next();
            reusable.update(entry.getKey(), entry.getNamespace(), entry.getState());
            return reusable;
        }

        private class ReusableEntry implements StateEntry<K, N, S> {
            private K key;
            private N namespace;
            private S state;

            void update(K key, N namespace, S state) {
                this.key = key;
                this.namespace = namespace;
                this.state = state;
            }

            @Override public K getKey() { return key; }
            @Override public N getNamespace() { return namespace; }
            @Override public S getState() { return state; }
        }
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        ArrayList<K> keys = new ArrayList<>();
        map.forEach((k, n, s) -> {
            if (namespace != null && namespace.equals(n)) {
                keys.add(k);
            }
        });
        return keys.stream();
    }

    // ================== For SwissMap access (used by snapshot) ==================

    /**
     * Returns the underlying SwissMap for snapshot traversal.
     */
    SwissMap<K, N, S> getSwissMap() {
        return map;
    }

    // ================== Statistics (for benchmark/testing) ==================

    /**
     * Gets detailed statistics about the state map.
     */
    public DetailedStats getDetailedStats() {
        return new DetailedStats(map.size(), map.getTables().size());
    }

    /**
     * Detailed statistics for monitoring.
     */
    public static class DetailedStats {
        public final int totalEntries;
        public final int tableCount;

        public DetailedStats(int totalEntries, int tableCount) {
            this.totalEntries = totalEntries;
            this.tableCount = tableCount;
        }

        @Override
        public String toString() {
            return String.format("DetailedStats{entries=%d, tables=%d}", totalEntries, tableCount);
        }
    }
}
