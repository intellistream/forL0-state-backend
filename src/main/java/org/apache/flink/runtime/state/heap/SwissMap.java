package org.apache.flink.runtime.state.heap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * SwissMap implementation for ForL0 State Backend.
 * 
 * <p>This is a high-performance hash map using Go 1.24 Swiss Tables architecture
 * with directory-based incremental expansion.
 * 
 * <p>Key features:
 * <ul>
 *   <li>SWAR parallel matching for fast lookups</li>
 *   <li>Directory + multiple tables for incremental growth</li>
 *   <li>87.5% load factor (7/8)</li>
 *   <li>Support for rehash, grow, and split operations</li>
 * </ul>
 * 
 * @param <K> key type
 * @param <N> namespace type
 * @param <S> state type
 */
class SwissMap<K, N, S> {

    // Initial configuration
    private static final int INITIAL_SLOT_COUNT = 64;  // 8 groups
    private static final int MAX_SLOT_COUNT = 1024;    // 128 groups per table
    private static final int INITIAL_ENTRY_CAPACITY = 64;
    private static final int INITIAL_FREE_LIST_SIZE = 16;

    // Counts
    private long used;          // total entry count
    private int globalDepth;    // directory bit count
    private int globalShift;    // 64 - globalDepth

    // Directory and tables
    private SwissTable[] directory;          // 2^globalDepth pointers
    private final List<SwissTable> tables;   // unique table set (for snapshot traversal)

    // Entry storage: keys and namespaces interleaved [K0, N0, K1, N1, ...]
    Object[] keys;
    Object[] states;
    private long[] entryHash;    // full 64-bit hash for rehash/grow/split
    private int[] freeList;
    private int freeCount;
    private int nextEntryId;

    /**
     * Creates a new SwissMap with default initial capacity.
     */
    SwissMap() {
        this.used = 0;
        this.globalDepth = 0;
        this.globalShift = 64;

        // Create initial single table
        SwissTable initialTable = new SwissTable(INITIAL_SLOT_COUNT, (byte) 0);
        this.directory = new SwissTable[1];
        this.directory[0] = initialTable;
        this.tables = new ArrayList<>();
        this.tables.add(initialTable);

        // Initialize entry storage
        this.keys = new Object[INITIAL_ENTRY_CAPACITY * 2];
        this.states = new Object[INITIAL_ENTRY_CAPACITY];
        this.entryHash = new long[INITIAL_ENTRY_CAPACITY];
        this.freeList = new int[INITIAL_FREE_LIST_SIZE];
        this.freeCount = 0;
        this.nextEntryId = 0;
    }

    // ========== Hash Computation ==========

    /**
     * Computes 64-bit hash for key and namespace using MurmurHash3 fmix64 finalizer.
     * 
     * This combines two 32-bit hashCodes into a full 64-bit value without information loss,
     * then applies the MurmurHash3 fmix64 finalizer for excellent avalanche properties.
     */
    private static long computeHash(Object key, Object namespace) {
        int kh = key.hashCode();
        int nh = namespace.hashCode();

        // Combine two 32-bit hashes into 64-bit (order-sensitive, lossless)
        // rotateLeft ensures namespace bits don't just overlay key bits
        // Golden ratio constant adds mixing even when one hash is zero
        long x = (kh & 0xFFFFFFFFL) ^ (Long.rotateLeft(nh & 0xFFFFFFFFL, 32) + 0x9e3779b97f4a7c15L);

        // MurmurHash3 fmix64 finalizer - excellent avalanche properties
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

    /**
     * Extracts H2 (low 7 bits) from hash.
     */
    private static int h2(long hash) {
        return (int) (hash & 0x7F);
    }

    /**
     * Extracts H1 (high 57 bits) from hash for group probing.
     */
    private static long h1(long hash) {
        return hash >>> 7;
    }

    // ========== Core Operations ==========

    /**
     * Returns the number of entries in the map.
     */
    public int size() {
        return (int) used;
    }

    /**
     * Gets the state value for the given key and namespace.
     * 
     * @return state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S get(K key, N namespace) {
        long hash = computeHash(key, namespace);
        int dirIdx = globalDepth == 0 ? 0 : (int) (hash >>> globalShift);
        SwissTable t = directory[dirIdx];

        int h2 = h2(hash);
        int group = (int) h1(hash) & t.groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = t.loadCtrlWord(group);
            int base = group << 3;

            // Match H2
            long match = SwissTable.matchH2(ctrlWord, h2);
            while (match != 0) {
                int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(match));
                int slot = base + lane;
                int entryId = t.slots[slot] - 1;
                if (entryId >= 0) {
                    int keyBase = entryId << 1;
                    K storedKey = (K) keys[keyBase];
                    N storedNs = (N) keys[keyBase + 1];
                    if (key.equals(storedKey) && namespace.equals(storedNs)) {
                        return (S) states[entryId];
                    }
                }
                match = SwissTable.clearLowestBit(match);
            }

            // Check for empty slot (early stop)
            if (SwissTable.matchEmpty(ctrlWord) != 0) {
                return null;
            }

            // Continue probing
            group = (group + stride) & t.groupMask;
            stride++;
        }
    }

    /**
     * Finds or inserts an entry for the given key and namespace.
     * Does NOT set the state - caller should set states[ptr-1] directly.
     * This enables efficient single-lookup put and transform operations.
     * 
     * @return entryId (1-based pointer)
     */
    public int put(K key, N namespace) {
        long hash = computeHash(key, namespace);
        return putInternal(key, namespace, hash);
    }

    @SuppressWarnings("unchecked")
    private int putInternal(K key, N namespace, long hash) {
        int dirIdx = globalDepth == 0 ? 0 : (int) (hash >>> globalShift);
        SwissTable t = directory[dirIdx];

        int h2 = h2(hash);
        int group = (int) h1(hash) & t.groupMask;
        int stride = 1;

        int firstDeletedSlot = -1;
        int firstEmptySlot = -1;

        while (true) {
            long ctrlWord = t.loadCtrlWord(group);
            int base = group << 3;

            // Check for existing key
            long match = SwissTable.matchH2(ctrlWord, h2);
            while (match != 0) {
                int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(match));
                int slot = base + lane;
                int entryId = t.slots[slot] - 1;
                if (entryId >= 0) {
                    int keyBase = entryId << 1;
                    K storedKey = (K) keys[keyBase];
                    N storedNs = (N) keys[keyBase + 1];
                    if (key.equals(storedKey) && namespace.equals(storedNs)) {
                        // Found existing entry - return ptr, caller sets state
                        return entryId + 1;
                    }
                }
                match = SwissTable.clearLowestBit(match);
            }

            // Record first deleted slot
            if (firstDeletedSlot == -1) {
                long delMask = SwissTable.matchDeleted(ctrlWord);
                if (delMask != 0) {
                    int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(delMask));
                    firstDeletedSlot = base + lane;
                }
            }

            // Check for empty slot
            long emptyMask = SwissTable.matchEmpty(ctrlWord);
            if (emptyMask != 0) {
                int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(emptyMask));
                firstEmptySlot = base + lane;
                break;
            }

            // Continue probing
            group = (group + stride) & t.groupMask;
            stride++;
        }

        // Determine insert position
        int insertSlot;
        boolean useDeleted;
        if (firstDeletedSlot != -1) {
            insertSlot = firstDeletedSlot;
            useDeleted = true;
        } else {
            insertSlot = firstEmptySlot;
            useDeleted = false;
            // Using EMPTY requires budget check
            if (t.growthLeft == 0) {
                rehashOrGrowOrSplit(t, dirIdx, hash);
                // Table may have changed, retry
                return putInternal(key, namespace, hash);
            }
        }

        // Allocate entry and insert
        int entryId = allocateEntry(key, namespace, hash);
        t.slots[insertSlot] = entryId + 1;
        t.ctrl[insertSlot] = (byte) h2;

        // Update counts
        t.used++;
        used++;
        if (useDeleted) {
            t.tomb--;
        } else {
            t.growthLeft--;
        }

        return entryId + 1;
    }

    /**
     * Removes the entry for the given key and namespace.
     * 
     * @return the removed state value, or null if not found
     */
    @SuppressWarnings("unchecked")
    public S remove(K key, N namespace) {
        long hash = computeHash(key, namespace);
        int dirIdx = globalDepth == 0 ? 0 : (int) (hash >>> globalShift);
        SwissTable t = directory[dirIdx];

        int h2 = h2(hash);
        int group = (int) h1(hash) & t.groupMask;
        int stride = 1;

        while (true) {
            long ctrlWord = t.loadCtrlWord(group);
            int base = group << 3;

            // Match H2
            long match = SwissTable.matchH2(ctrlWord, h2);
            while (match != 0) {
                int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(match));
                int slot = base + lane;
                int entryId = t.slots[slot] - 1;
                if (entryId >= 0) {
                    int keyBase = entryId << 1;
                    K storedKey = (K) keys[keyBase];
                    N storedNs = (N) keys[keyBase + 1];
                    if (key.equals(storedKey) && namespace.equals(storedNs)) {
                        // Found - determine delete strategy BEFORE modifying ctrl
                        boolean hasEmpty = SwissTable.matchEmpty(ctrlWord) != 0;

                        // Get old state
                        S oldState = (S) states[entryId];

                        // Clear entry
                        keys[keyBase] = null;
                        keys[keyBase + 1] = null;
                        states[entryId] = null;
                        freeEntry(entryId);

                        // Clear slot
                        t.slots[slot] = 0;

                        // Update ctrl and counts
                        if (hasEmpty) {
                            t.ctrl[slot] = SwissTable.CTRL_EMPTY;
                            t.used--;
                            t.growthLeft++;
                        } else {
                            t.ctrl[slot] = SwissTable.CTRL_DELETED;
                            t.used--;
                            t.tomb++;
                        }
                        used--;

                        return oldState;
                    }
                }
                match = SwissTable.clearLowestBit(match);
            }

            // Check for empty slot (early stop)
            if (SwissTable.matchEmpty(ctrlWord) != 0) {
                return null;
            }

            // Continue probing
            group = (group + stride) & t.groupMask;
            stride++;
        }
    }

    // ========== Entry Management ==========

    private int allocateEntry(K key, N namespace, long hash) {
        int entryId;
        if (freeCount > 0) {
            entryId = freeList[--freeCount];
        } else {
            ensureEntryCapacity();
            entryId = nextEntryId++;
        }

        int keyBase = entryId << 1;
        keys[keyBase] = key;
        keys[keyBase + 1] = namespace;
        // Note: state is NOT set here - caller sets states[ptr-1] directly
        entryHash[entryId] = hash;

        return entryId;
    }

    private void freeEntry(int entryId) {
        if (freeCount >= freeList.length) {
            freeList = Arrays.copyOf(freeList, freeList.length * 2);
        }
        freeList[freeCount++] = entryId;
    }

    private void ensureEntryCapacity() {
        if (nextEntryId >= states.length) {
            int newCapacity = states.length * 2;
            keys = Arrays.copyOf(keys, newCapacity * 2);
            states = Arrays.copyOf(states, newCapacity);
            entryHash = Arrays.copyOf(entryHash, newCapacity);
        }
    }

    // ========== Expansion Operations ==========

    private void rehashOrGrowOrSplit(SwissTable t, int dirIdx, long hash) {
        if (t.tomb > 0) {
            rehashSameCapacity(t);
        } else if (t.capacity < MAX_SLOT_COUNT) {
            grow(t);
        } else {
            // Split
            int oldLocalDepth = t.localDepth;
            if (oldLocalDepth == globalDepth) {
                growDirectory();
                // Recalculate dirIdx after directory grows
                dirIdx = globalDepth == 0 ? 0 : (int) (hash >>> globalShift);
            }
            split(t, dirIdx, oldLocalDepth);
        }
    }

    /**
     * Rehash with same capacity to clear tombstones.
     */
    private void rehashSameCapacity(SwissTable t) {
        int capacity = t.capacity;
        byte[] newCtrl = new byte[capacity];
        Arrays.fill(newCtrl, SwissTable.CTRL_EMPTY);
        int[] newSlots = new int[capacity];
        int groupMask = t.groupMask;

        // Reinsert all FULL entries
        for (int i = 0; i < capacity; i++) {
            if (SwissTable.isFull(t.ctrl[i])) {
                int entryId = t.slots[i] - 1;
                long hash = entryHash[entryId];
                insertEntryIntoArrays(newCtrl, newSlots, groupMask, entryId, hash);
            }
        }

        t.ctrl = newCtrl;
        t.slots = newSlots;
        t.tomb = 0;
        t.growthLeft = (short) (SwissTable.maxOcc(capacity) - t.used);
    }

    /**
     * Grow table to double capacity.
     */
    private void grow(SwissTable t) {
        int newCapacity = t.capacity * 2;
        byte[] newCtrl = new byte[newCapacity];
        Arrays.fill(newCtrl, SwissTable.CTRL_EMPTY);
        int[] newSlots = new int[newCapacity];
        int newGroupMask = (newCapacity >>> 3) - 1;

        // Reinsert all FULL entries
        for (int i = 0; i < t.capacity; i++) {
            if (SwissTable.isFull(t.ctrl[i])) {
                int entryId = t.slots[i] - 1;
                long hash = entryHash[entryId];
                insertEntryIntoArrays(newCtrl, newSlots, newGroupMask, entryId, hash);
            }
        }

        t.capacity = (short) newCapacity;
        t.groupMask = newGroupMask;
        t.ctrl = newCtrl;
        t.slots = newSlots;
        t.tomb = 0;
        t.growthLeft = (short) (SwissTable.maxOcc(newCapacity) - t.used);
    }

    /**
     * Split table into two when at max capacity.
     */
    private void split(SwissTable oldTable, int dirIdx, int oldLocalDepth) {
        byte newLocalDepth = (byte) (oldLocalDepth + 1);
        SwissTable left = new SwissTable(INITIAL_SLOT_COUNT, newLocalDepth);
        SwissTable right = new SwissTable(INITIAL_SLOT_COUNT, newLocalDepth);

        int bitPos = 64 - newLocalDepth;

        // Distribute entries
        for (int i = 0; i < oldTable.capacity; i++) {
            if (SwissTable.isFull(oldTable.ctrl[i])) {
                int entryId = oldTable.slots[i] - 1;
                long hash = entryHash[entryId];
                int side = (int) ((hash >>> bitPos) & 1L);
                SwissTable target = (side == 0) ? left : right;
                insertIntoTable(target, hash, entryId);
            }
        }

        // Install split
        installSplit(dirIdx, oldTable, oldLocalDepth, left, right);
    }

    /**
     * Inserts an entry into ctrl/slots arrays using quadratic probing.
     * Uses labeled break for efficient loop exit.
     *
     * @param ctrl the control byte array
     * @param slots the slot pointer array
     * @param groupMask the group mask for modulo
     * @param entryId the entry ID to insert
     * @param hash the full 64-bit hash
     */
    private void insertEntryIntoArrays(byte[] ctrl, int[] slots, int groupMask,
                                        int entryId, long hash) {
        int h2 = h2(hash);
        int group = (int) h1(hash) & groupMask;
        int stride = 1;

        PROBE: while (true) {
            int base = group << 3;
            for (int j = 0; j < 8; j++) {
                int slot = base + j;
                if (ctrl[slot] == SwissTable.CTRL_EMPTY) {
                    ctrl[slot] = (byte) h2;
                    slots[slot] = entryId + 1;
                    break PROBE;  // Labeled break for direct exit
                }
            }
            group = (group + stride) & groupMask;
            stride++;
        }
    }

    private void insertIntoTable(SwissTable t, long hash, int entryId) {
        // Ensure capacity (may need to grow during split)
        while (t.growthLeft == 0 && t.capacity < MAX_SLOT_COUNT) {
            grow(t);
        }

        // Insert using the common method
        insertEntryIntoArrays(t.ctrl, t.slots, t.groupMask, entryId, hash);
        t.used++;
        t.growthLeft--;
    }

    private void installSplit(int dirIdx, SwissTable oldTable, int oldLocalDepth,
                              SwissTable left, SwissTable right) {
        int spanOld = 1 << (globalDepth - oldLocalDepth);
        int start = (dirIdx / spanOld) * spanOld;
        int mid = start + (spanOld >>> 1);

        // Update directory
        for (int i = start; i < mid; i++) {
            directory[i] = left;
        }
        for (int i = mid; i < start + spanOld; i++) {
            directory[i] = right;
        }

        // Update tables list
        tables.remove(oldTable);
        tables.add(left);
        tables.add(right);
    }

    private void growDirectory() {
        SwissTable[] newDir = new SwissTable[directory.length * 2];
        for (int i = 0; i < directory.length; i++) {
            newDir[2 * i] = directory[i];
            newDir[2 * i + 1] = directory[i];
        }
        directory = newDir;
        globalDepth++;
        globalShift--;
    }

    // ========== Iteration ==========

    /**
     * Returns the list of unique tables for snapshot traversal.
     */
    List<SwissTable> getTables() {
        return tables;
    }

    /**
     * Iterates over all entries.
     */
    Iterator<SwissMapEntry<K, N, S>> iterator() {
        return new SwissMapIterator();
    }

    /**
     * Entry class for iteration.
     */
    static class SwissMapEntry<K, N, S> {
        final K key;
        final N namespace;
        final S state;

        SwissMapEntry(K key, N namespace, S state) {
            this.key = key;
            this.namespace = namespace;
            this.state = state;
        }

        public K getKey() { return key; }
        public N getNamespace() { return namespace; }
        public S getState() { return state; }
    }

    /**
     * Iterator using SWAR matchFull for efficient parallel slot scanning.
     */
    private class SwissMapIterator implements Iterator<SwissMapEntry<K, N, S>> {
        private int tableIdx = 0;
        private int groupIdx = -1;  // Start at -1 so first increment goes to 0
        private long currentMask = 0;  // SWAR bitmask for current group
        private SwissMapEntry<K, N, S> nextEntry = null;

        SwissMapIterator() {
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public SwissMapEntry<K, N, S> next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            SwissMapEntry<K, N, S> result = nextEntry;
            advance();
            return result;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            nextEntry = null;

            while (tableIdx < tables.size()) {
                SwissTable t = tables.get(tableIdx);
                int groupCount = t.groupMask + 1;

                // Process remaining bits in current group's mask
                while (currentMask != 0) {
                    int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(currentMask));
                    int slot = (groupIdx << 3) + lane;
                    currentMask = SwissTable.clearLowestBit(currentMask);

                    int entryId = t.slots[slot] - 1;
                    if (entryId >= 0) {
                        int keyBase = entryId << 1;
                        K key = (K) keys[keyBase];
                        N namespace = (N) keys[keyBase + 1];
                        S state = (S) states[entryId];
                        nextEntry = new SwissMapEntry<>(key, namespace, state);
                        return;
                    }
                }

                // Move to next group with SWAR matchFull
                groupIdx++;
                while (groupIdx < groupCount) {
                    long ctrlWord = t.loadCtrlWord(groupIdx);
                    currentMask = SwissTable.matchFull(ctrlWord);
                    if (currentMask != 0) {
                        // Found a group with FULL slots, process first one
                        int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(currentMask));
                        int slot = (groupIdx << 3) + lane;
                        currentMask = SwissTable.clearLowestBit(currentMask);

                        int entryId = t.slots[slot] - 1;
                        if (entryId >= 0) {
                            int keyBase = entryId << 1;
                            K key = (K) keys[keyBase];
                            N namespace = (N) keys[keyBase + 1];
                            S state = (S) states[entryId];
                            nextEntry = new SwissMapEntry<>(key, namespace, state);
                            return;
                        }
                    }
                    groupIdx++;
                }

                // Move to next table
                tableIdx++;
                groupIdx = -1;  // Reset to -1 so first increment goes to 0
                currentMask = 0;
            }
        }
    }

    /**
     * Applies a function to each entry using SWAR matchFull for efficiency.
     */
    void forEach(EntryConsumer<K, N, S> consumer) {
        for (SwissTable t : tables) {
            int groupCount = t.groupMask + 1;
            for (int g = 0; g < groupCount; g++) {
                long ctrlWord = t.loadCtrlWord(g);
                long full = SwissTable.matchFull(ctrlWord);
                int base = g << 3;
                while (full != 0) {
                    int lane = SwissTable.laneFromTz(Long.numberOfTrailingZeros(full));
                    int slot = base + lane;
                    full = SwissTable.clearLowestBit(full);

                    int entryId = t.slots[slot] - 1;
                    int keyBase = entryId << 1;
                    @SuppressWarnings("unchecked")
                    K key = (K) keys[keyBase];
                    @SuppressWarnings("unchecked")
                    N namespace = (N) keys[keyBase + 1];
                    @SuppressWarnings("unchecked")
                    S state = (S) states[entryId];
                    consumer.accept(key, namespace, state);
                }
            }
        }
    }

    @FunctionalInterface
    interface EntryConsumer<K, N, S> {
        void accept(K key, N namespace, S state);
    }
}
