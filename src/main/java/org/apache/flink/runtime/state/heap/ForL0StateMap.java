package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * ForL0StateMap implementation using Swiss Tables architecture.
 * 
 * <p>This implementation uses a directory-based Swiss Table structure for
 * high-performance key-value state storage with incremental expansion.
 * 
 * <p>Architecture (aligned with Go 1.24 Swiss Tables):
 * <ul>
 *   <li>ForL0StateMap: Directory routing + StateMap interface implementation</li>
 *   <li>SwissTable: Self-contained storage with SWAR parallel matching</li>
 * </ul>
 * 
 * <p>Hash bit allocation:
 * <ul>
 *   <li>H1 = hash >>> 7 (high 57 bits, for probe starting group)</li>
 *   <li>H2 = hash & 0x7F (low 7 bits, stored in ctrl byte)</li>
 *   <li>Directory routing uses high globalDepth bits</li>
 * </ul>
 *
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
public class ForL0StateMap<K, N, S> extends StateMap<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateMap.class);

    // ========== Configuration ==========
    private static final int INITIAL_TABLE_CAPACITY = 64;
    private static final int MAX_TABLE_CAPACITY = 1024;

    // ========== Global count ==========
    private int size;

    // ========== Directory ==========
    private int globalDepth;
    private int globalShift;  // 64 - globalDepth
    private AbstractSwissTable<K, N, S>[] directory;
    private final List<AbstractSwissTable<K, N, S>> tables;  // Unique table list for snapshot/iteration

    /**
     * The last namespace that was actually inserted. This is a small optimization to reduce
     * duplicate namespace objects (same as CopyOnWriteStateMap).
     */
    private N lastNamespace;

    /**
     * Creates a new ForL0StateMap with Swiss Tables architecture.
     * Uses generic SwissTable implementation (fallback).
     */
    @SuppressWarnings("unchecked")
    public ForL0StateMap() {
        this.size = 0;
        this.globalDepth = 0;
        this.globalShift = 64;
        
        // Create initial single table (default to generic implementation)
        AbstractSwissTable<K, N, S> initTable = new SwissTableGeneric<>(INITIAL_TABLE_CAPACITY, (byte) 0, 0);
        this.directory = new AbstractSwissTable[] { initTable };
        this.tables = new ArrayList<>();
        this.tables.add(initTable);
        
    }

    /**
     * Creates a new ForL0StateMap with specialized SwissTable based on key/namespace types.
     * 
     * <p>Usage examples:
     * <pre>
     *   new ForL0StateMap<>(Long.class, VoidNamespace.class)   // → SwissTableLongVoid
     *   new ForL0StateMap<>(Long.class, TimeWindow.class)      // → SwissTableLongTimeWindow
     *   new ForL0StateMap<>(String.class, VoidNamespace.class) // → SwissTableStringVoid
     *   new ForL0StateMap<>(String.class, TimeWindow.class)    // → SwissTableStringTimeWindow
     * </pre>
     * 
     * @param keyClass the key Class (e.g., Long.class, String.class)
     * @param namespaceClass the namespace Class (e.g., VoidNamespace.class, TimeWindow.class)
     */
    @SuppressWarnings("unchecked")
    public ForL0StateMap(Class<?> keyClass, Class<?> namespaceClass) {
        this.size = 0;
        this.globalDepth = 0;
        this.globalShift = 64;
        
        AbstractSwissTable<K, N, S> initTable = createTable(keyClass, namespaceClass, 
                INITIAL_TABLE_CAPACITY, (byte) 0, 0);
        this.directory = new AbstractSwissTable[] { initTable };
        this.tables = new ArrayList<>();
        this.tables.add(initTable);
        
        LOG.info("ForL0StateMap created with {} (key={}, namespace={})",
                initTable.getClass().getSimpleName(),
                keyClass.getSimpleName(),
                namespaceClass.getSimpleName());
    }

    /**
     * Factory method to create the appropriate SwissTable specialization.
     */
    @SuppressWarnings("unchecked")
    private static <K, N, S> AbstractSwissTable<K, N, S> createTable(
            Class<?> keyClass, Class<?> nsClass, int capacity, byte localDepth, int index) {
        
        if (keyClass == Long.class) {
            if (nsClass == VoidNamespace.class) {
                return (AbstractSwissTable<K, N, S>) new SwissTableLongVoid<>(capacity, localDepth, index);
            }
            if (nsClass == TimeWindow.class) {
                return (AbstractSwissTable<K, N, S>) new SwissTableLongTimeWindow<>(capacity, localDepth, index);
            }
        }
        
        if (keyClass == String.class) {
            if (nsClass == VoidNamespace.class) {
                return (AbstractSwissTable<K, N, S>) new SwissTableStringVoid<>(capacity, localDepth, index);
            }
            if (nsClass == TimeWindow.class) {
                return (AbstractSwissTable<K, N, S>) new SwissTableStringTimeWindow<>(capacity, localDepth, index);
            }
        }
        
        return new SwissTableGeneric<>(capacity, localDepth, index);
    }

    @Override
    public void close() throws Exception {
        LOG.info("ForL0StateMap closing - size: {}", size);
    }

    // ========== Hash Computation ==========

    /**
     * Computes 64-bit hash for key and namespace.
     * 
     * Uses bitMix on key + lightweight ns spread to break XOR symmetry.
     */
    private static long computeHash(Object key, Object namespace) {
        int h = MathUtils.bitMix(key.hashCode());
        int n = namespace.hashCode() * 0x9E3779B9;
        h ^= n ^ (n >>> 16);
        long x = (h & 0xFFFFFFFFL) * 0x9e3779b97f4a7c15L;
        return x ^ (x >>> 32);
    }

    // ========== Directory Routing ==========

    /**
     * Locates the table for the given hash.
     */
    private AbstractSwissTable<K, N, S> locateTable(long hash) {
        int dirIdx = globalDepth == 0 ? 0 : (int)(hash >>> globalShift);
        return directory[dirIdx];
    }

    // ========== StateMap Interface Implementation ==========

    @Override
    public int size() {
        return size;
    }

    @Override
    public S get(K key, N namespace) {
        long hash = computeHash(key, namespace);
        AbstractSwissTable<K, N, S> t = locateTable(hash);
        return t.get(hash, key, namespace);
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
        
        long hash = computeHash(key, namespace);
        
        // Outer retry loop (aligned with Go 1.24: outer: for { ... continue outer })
        outer:
        while (true) {
            AbstractSwissTable<K, N, S> t = locateTable(hash);
            int result = t.put(hash, key, namespace, MAX_TABLE_CAPACITY);
            
            // Handle signals from AbstractSwissTable
            switch (result) {
                case AbstractSwissTable.NEED_REHASH:
                    t.rehash();
                    continue outer;
                case AbstractSwissTable.NEED_GROW:
                    t.grow();
                    continue outer;
                case AbstractSwissTable.NEED_SPLIT:
                    handleSplit(t);
                    continue outer;
            }
            
            int slot = result & AbstractSwissTable.SLOT_MASK;
            boolean isNew = (result & AbstractSwissTable.NEW_FLAG) != 0;
            
            t.values[slot] = state;
            if (isNew) {
                size++;
            }
            return;
        }
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
        
        long hash = computeHash(key, namespace);
        
        // Outer retry loop (aligned with Go 1.24: outer: for { ... continue outer })
        outer:
        while (true) {
            AbstractSwissTable<K, N, S> t = locateTable(hash);
            int result = t.put(hash, key, namespace, MAX_TABLE_CAPACITY);
            
            // Handle signals from AbstractSwissTable
            switch (result) {
                case AbstractSwissTable.NEED_REHASH:
                    t.rehash();
                    continue outer;
                case AbstractSwissTable.NEED_GROW:
                    t.grow();
                    continue outer;
                case AbstractSwissTable.NEED_SPLIT:
                    handleSplit(t);
                    continue outer;
            }
            
            int slot = result & AbstractSwissTable.SLOT_MASK;
            boolean isNew = (result & AbstractSwissTable.NEW_FLAG) != 0;
            
            S oldState = isNew ? null : (S) t.values[slot];
            t.values[slot] = state;
            if (isNew) {
                size++;
            }
            return oldState;
        }
    }

    @Override
    public void remove(K key, N namespace) {
        long hash = computeHash(key, namespace);
        AbstractSwissTable<K, N, S> t = locateTable(hash);
        S old = t.remove(hash, key, namespace);
        if (old != null) {
            size--;
        }
    }

    @Override
    public S removeAndGetOld(K key, N namespace) {
        long hash = computeHash(key, namespace);
        AbstractSwissTable<K, N, S> t = locateTable(hash);
        S old = t.remove(hash, key, namespace);
        if (old != null) {
            size--;
        }
        return old;
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
        
        long hash = computeHash(key, namespace);
        
        // Outer retry loop (aligned with Go 1.24: outer: for { ... continue outer })
        outer:
        while (true) {
            AbstractSwissTable<K, N, S> t = locateTable(hash);
            int result = t.put(hash, key, namespace, MAX_TABLE_CAPACITY);
            
            // Handle signals from AbstractSwissTable
            switch (result) {
                case AbstractSwissTable.NEED_REHASH:
                    t.rehash();
                    continue outer;
                case AbstractSwissTable.NEED_GROW:
                    t.grow();
                    continue outer;
                case AbstractSwissTable.NEED_SPLIT:
                    handleSplit(t);
                    continue outer;
            }
            
            int slot = result & AbstractSwissTable.SLOT_MASK;
            boolean isNew = (result & AbstractSwissTable.NEW_FLAG) != 0;
            
            S oldState = isNew ? null : (S) t.values[slot];
            t.values[slot] = transformation.apply(oldState, value);
            
            if (isNew) {
                size++;
            }
            return;
        }
    }

    @Override
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (AbstractSwissTable<K, N, S> t : tables) {
            count += t.countNamespace(namespace);
        }
        return count;
    }

    @Override
    public Stream<K> getKeys(N namespace) {
        ArrayList<K> keys = new ArrayList<>();
        for (AbstractSwissTable<K, N, S> t : tables) {
            for (int i = 0; i < t.capacity; i++) {
                if (t.isSlotFull(i)) {
                    N ns = t.getNamespace(i);
                    if (namespace.equals(ns)) {
                        K key = t.getKey(i);
                        keys.add(key);
                    }
                }
            }
        }
        return keys.stream();
    }

    // ========== Split Operations ==========

    /**
     * Handles NEED_SPLIT signal from a table.
     */
    private void handleSplit(AbstractSwissTable<K, N, S> t) {
        // If localDepth == globalDepth, need to grow directory first
        if (t.localDepth == globalDepth) {
            growDirectory();
        }
        split(t);
    }

    /**
     * Directory doubling.
     * 
     * Go-style deduplication: same table may be pointed to by multiple directory slots.
     * By checking t.index == i, we ensure each table's index is updated only once.
     */
    @SuppressWarnings("unchecked")
    private void growDirectory() {
        AbstractSwissTable<K, N, S>[] newDir = new AbstractSwissTable[directory.length * 2];
        for (int i = 0; i < directory.length; i++) {
            AbstractSwissTable<K, N, S> t = directory[i];
            newDir[2 * i] = t;
            newDir[2 * i + 1] = t;
            // Go-style deduplication: only update index on first encounter
            if (t.index == i) {
                t.index = 2 * i;
            }
        }
        directory = newDir;
        globalDepth++;
        globalShift--;
    }

    /**
     * Split a table into two new tables.
     * 
     * Key points:
     * 1. New table capacity = old table capacity (same-capacity split)
     * 2. Split bit must align with directory routing
     * 3. Use oldTable.index to locate directory range
     */
    @SuppressWarnings("unchecked")
    private void split(AbstractSwissTable<K, N, S> oldTable) {
        byte newLocalDepth = (byte)(oldTable.localDepth + 1);
        
        // Calculate new table indices in directory
        // oldTable covers directory range: [index, index + span)
        int span = 1 << (globalDepth - oldTable.localDepth);
        int leftIndex = oldTable.index;
        int rightIndex = oldTable.index + (span >>> 1);
        
        // New tables have same capacity as old table (use the same concrete type as oldTable)
        AbstractSwissTable<K, N, S> left = oldTable.createNew(oldTable.capacity, newLocalDepth, leftIndex);
        AbstractSwissTable<K, N, S> right = oldTable.createNew(oldTable.capacity, newLocalDepth, rightIndex);
        
        // Split bit must align with directory routing rule
        // Directory uses high globalDepth bits of hash
        // Split adds 1 bit to distinguish left/right
        int splitShift = 64 - newLocalDepth;
        
        // Iterate old table, distribute to new tables
        for (int slot = 0; slot < oldTable.capacity; slot++) {
            if (oldTable.isSlotFull(slot)) {
                K key = oldTable.getKey(slot);
                N ns = oldTable.getNamespace(slot);
                S value = (S) oldTable.values[slot];
                
                // Use stored hash (no need to recompute)
                long hash = oldTable.hashes[slot];
                
                // Get split bit
                int side = (int)((hash >>> splitShift) & 1L);
                
                AbstractSwissTable<K, N, S> target = (side == 0) ? left : right;
                target.putDirect(hash, key, ns, value);
            }
        }
        
        // Update directory
        // oldTable covers [index, index + span)
        // left covers [index, index + span/2)
        // right covers [index + span/2, index + span)
        int mid = oldTable.index + (span >>> 1);
        for (int i = oldTable.index; i < mid; i++) {
            directory[i] = left;
        }
        for (int i = mid; i < oldTable.index + span; i++) {
            directory[i] = right;
        }
        
        // Update tables list
        tables.remove(oldTable);
        tables.add(left);
        tables.add(right);
    }

    // ========== Iteration ==========

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
        return new SwissTableIterator();
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
     * Iterator that traverses all SwissTables.
     */
    private class SwissTableIterator implements Iterator<StateEntry<K, N, S>> {
        private int tableIdx = 0;
        private int slotIdx = 0;
        private K nextKey = null;
        private N nextNamespace = null;
        private S nextState = null;

        SwissTableIterator() {
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public StateEntry<K, N, S> next() {
            if (nextKey == null) {
                throw new NoSuchElementException();
            }
            // Capture current values before advancing
            K key = nextKey;
            N namespace = nextNamespace;
            S state = nextState;
            advance();
            return new StateEntryImpl<>(key, namespace, state);
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            nextKey = null;
            nextNamespace = null;
            nextState = null;

            while (tableIdx < tables.size()) {
                AbstractSwissTable<K, N, S> t = tables.get(tableIdx);

                // Find next FULL slot
                while (slotIdx < t.capacity) {
                    if (t.isSlotFull(slotIdx)) {
                        nextKey = t.getKey(slotIdx);
                        nextNamespace = t.getNamespace(slotIdx);
                        nextState = (S) t.values[slotIdx];
                        slotIdx++;
                        return;
                    }
                    slotIdx++;
                }

                // Move to next table
                tableIdx++;
                slotIdx = 0;
            }
        }
    }

    @Nonnull
    @Override
    public ForL0StateMapSnapshot<K, N, S> stateSnapshot() {
        return new ForL0StateMapSnapshot<>(this);
    }

    // ========== For snapshot access ==========

    /**
     * Returns the list of unique tables for snapshot traversal.
     */
    List<AbstractSwissTable<K, N, S>> getTables() {
        return tables;
    }

    /**
     * Returns the number of tables for testing/monitoring.
     */
    public int getTableCount() {
        return tables.size();
    }

    // ========== Statistics (for benchmark/testing) ==========

    /**
     * Gets detailed statistics about the state map.
     */
    public DetailedStats getDetailedStats() {
        return new DetailedStats(size, tables.size());
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
