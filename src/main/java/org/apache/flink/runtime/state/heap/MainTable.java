package org.apache.flink.runtime.state.heap;

import org.apache.flink.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Main Table implementation for ForL0 State Backend.
 * 
 * <p>This is the heap-based version using flattened one-dimensional arrays for optimal cache performance.
 * Each slot is a single long: [Hash(32b) | Ptr(32b)].
 * 
 * <p>64-byte aligned buckets with 7 data slots + 1 extension long.
 * Supports tree-like expansion and global resize.
 * 
 * <p>Entry storage uses flattened AoS (Array of Structures) layout: entries are stored as
 * consecutive K, N, S triplets in Object[] for spatial locality, avoiding object overhead.
 * 
 * <p>Optionally manages an L0Table as a hot cache layer. When L0Table is present,
 * get() checks L0 first and put() updates L0 cache automatically.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class MainTable<K, N, S> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MainTable.class);

    // Bucket layout constants
    private static final int BUCKET_SIZE_BITS = 3;       // 8 longs = 2^3
    private static final int SLOTS_PER_BUCKET = 7;       // 7 data slots
    private static final int EXTENSION_SLOT_INDEX = 7;   // 8th long for extension pointers
    
    // Initial table size: 32K buckets = 256K longs = 2MB (balance between memory and resize cost)
    static final int INITIAL_BUCKET_COUNT = 32 * 1024;  // Package-private for testing
    
    // Slot bit operations (Hash in high 32 bits, Ptr in low 32 bits)
    private static final int HASH_SHIFT = 32;
    private static final long PTR_MASK = 0xFFFFFFFFL;
    
    // Extension bucket slot layout: 8 x 8-bit pointers in one long (64 bits fully utilized)
    private static final int EXT_PTR_MASK = 0xFF;
    // Use high 3 bits of hash to select extension path (low bits are same for all entries in a bucket)
    private static final int EXT_INDEX_SHIFT = 29;
    private static final int EXT_INDEX_MASK = 0x7;
    private static final int MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET = 255;
    private static final int EXTENSION_AREA_SIZE_BITS = 8;  // 2^8 = 256 ≈ 255 for shift optimization
    
    private static final double DEFAULT_LOAD_FACTOR_THRESHOLD = 1.5;
    private static final int EXTENSION_POOL_GROW_SIZE = 32;  // Grow 32 extension areas at a time

    // ========== Entry Storage (Key/Namespace separated from State) ==========
    
    /** Initial entry capacity: 32K entries to match bucket count. */
    private static final int INITIAL_ENTRY_CAPACITY = 32 * 1024;
    
    /** Initial free list size. */
    private static final int INITIAL_FREE_LIST_SIZE = 512;

    /** Key and namespace storage: [K0, N0, K1, N1, ...]. Package-private for direct access. */
    Object[] keyNs;
    
    /** State storage: [S0, S1, ...]. Package-private for direct access. */
    Object[] states;
    
    /** Hash values stored separately (only accessed during rehash). */
    private int[] hashes;
    
    /** Current capacity (entries.length / ENTRY_STRIDE). */
    private int entryCapacity;
    
    /** Next allocation index for entries. */
    private int nextEntryIndex;
    
    /** Free list for deleted entry slots. */
    private int[] freeList;
    private int freeCount;

    // ========== Bucket Index Storage ==========
    
    // Flattened bucket table: one-dimensional array for better cache performance
    long[] table;            // Main bucket table (package-private for iterator)
    long[] extensions;       // Extension bucket area (package-private for iterator)
    int bucketCount;         // Package-private for iterator
    private int bucketMask;  // bucketCount - 1, cached for fast modulo
    
    // Extension bucket management
    private int[] extensionBucketCounts;       // Number of extension buckets per main bucket
    int[] extensionBucketBaseIndices;  // Base bucket index (package-private for iterator)
    private int extensionPoolCapacity;         // Total extension areas available
    private int extensionPoolUsed;             // Extension areas allocated to main buckets
    
    private final double loadFactorThreshold;
    private boolean needsResize = false;
    private int totalEntries = 0;
    private int maxExtensionBucketsUsed = 0;
    private int maxExtensionDepth = 0;  // True tree depth (traversal count)
    private boolean closed = false;

    // L0 cache layer (optional)
    @Nullable
    private final L0Table<K, N, S> l0Table;
    private final boolean l0CacheEnabled;  // Final flag for JIT branch elimination

    /**
     * Creates a MainTable with default load factor threshold and no L0 cache.
     */
    public MainTable() {
        this(DEFAULT_LOAD_FACTOR_THRESHOLD, null);
    }

    /**
     * Creates a MainTable with custom load factor threshold and no L0 cache.
     *
     * @param loadFactorThreshold the load factor threshold for triggering resize
     */
    public MainTable(double loadFactorThreshold) {
        this(loadFactorThreshold, null);
    }

    /**
     * Creates a MainTable with custom load factor threshold and optional L0 cache.
     *
     * @param loadFactorThreshold the load factor threshold for triggering resize
     * @param l0Table optional L0 cache (can be null)
     */
    public MainTable(double loadFactorThreshold, @Nullable L0Table<K, N, S> l0Table) {
        this.loadFactorThreshold = loadFactorThreshold;
        this.l0Table = l0Table;
        this.l0CacheEnabled = (l0Table != null);  // Final for JIT optimization
        
        // Initialize bucket table: 32K buckets = 2MB (balance between initial cost and resize overhead)
        this.bucketCount = INITIAL_BUCKET_COUNT;
        this.bucketMask = INITIAL_BUCKET_COUNT - 1;
        this.table = new long[bucketCount << BUCKET_SIZE_BITS];
        this.extensions = null;  // Allocated on demand
        
        // Initialize extension management arrays
        this.extensionBucketBaseIndices = new int[bucketCount];
        this.extensionBucketCounts = new int[bucketCount];
        this.extensionPoolCapacity = 0;
        this.extensionPoolUsed = 0;
        
        // Initialize entry storage: keyNs (64K Object refs) + states (32K refs) + hashes (32K ints)
        this.entryCapacity = INITIAL_ENTRY_CAPACITY;
        this.keyNs = new Object[entryCapacity * 2];
        this.states = new Object[entryCapacity];
        this.hashes = new int[entryCapacity];
        this.nextEntryIndex = 0;
        this.freeList = new int[INITIAL_FREE_LIST_SIZE];
        this.freeCount = 0;
    }

    // ========== Hash Computation ==========

    /**
     * Computes composite hash for key and namespace.
     * @param key the key object
     * @param namespace the namespace object
     * @return scrambled composite hash value
     */
    public static int computeHash(Object key, Object namespace) {
        int keyHash = key.hashCode();
        int nsHash = namespace.hashCode();
        // Mix using golden ratio constant (0x9e3779b9) with bit shifts
        keyHash ^= nsHash + 0x9e3779b9 + (keyHash << 6) + (keyHash >>> 2);
        return MathUtils.bitMix(keyHash);
    }

    // ========== Core Operations ==========

    /**
     * Gets the state value from the table. Checks L0 cache first if present.
     *
     * @param key the key object
     * @param namespace the namespace object
     * @return the state value if found, null otherwise
     */
    @Nullable
    public S get(K key, N namespace) {
        int hash = computeHash(key, namespace);
        
        // Check L0 cache first (JIT can eliminate this branch if l0CacheEnabled is false)
        if (l0CacheEnabled) {
            int ptr = l0Table.get(hash, key, namespace, keyNs);
            if (ptr > 0) {
                return (S) states[ptr - 1];  // Return state
            }
        }
        
        // Search in MainTable - Fast path: search main bucket first
        int mainBucketIndex = hash & bucketMask;
        int tableOffset = mainBucketIndex << BUCKET_SIZE_BITS;
        
        // Search main bucket slots with Early Exit
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = table[tableOffset + i];
            if (slot == 0) break;  // Early exit: no more entries in this bucket
            if ((int)(slot >>> HASH_SHIFT) != hash) continue;  // Hash mismatch

            int ptr = (int) slot;
            int base = (ptr - 1) * 2;
            Object entryKey = keyNs[base];
            if (entryKey != key && !entryKey.equals(key)) continue;

            Object entryNs = keyNs[base + 1];
            if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
            if (l0CacheEnabled) {
                l0Table.put(hash, ptr);
            }
            return (S) states[ptr - 1]; // Found
        }
        
        // Check if main bucket has extension
        long extLong = table[tableOffset + EXTENSION_SLOT_INDEX];
        int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
        int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
        if (extOffset == 0) {
            return null;  // Not found, no extension
        }
        
        // Slow path: search extension buckets
        int bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        while (true) {
            int extTableOffset = (bucketIndex - bucketCount) << BUCKET_SIZE_BITS;
            
            // Search extension bucket slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = extensions[extTableOffset + i];
                if (slot == 0) break;
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;

                int ptr = (int) slot;
                int base = (ptr - 1) * 2;
                Object entryKey = keyNs[base];
                if (entryKey != key && !entryKey.equals(key)) continue;

                Object entryNs = keyNs[base + 1];
                if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                return (S) states[ptr - 1];
            }
            
            // Check next level extension
            extLong = extensions[extTableOffset + EXTENSION_SLOT_INDEX];
            extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return null;  // Not found
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    /**
     * Inserts or finds an entry and returns its ptr.
     * State is initialized to null for new entries.
     *
     * @param key the key object
     * @param namespace the namespace object
     * @return ptr of the entry (always positive)
     */
    public int put(K key, N namespace) {
        int hash = computeHash(key, namespace);
        // Auto-resize before insertion if needed
        if (needsResize) {
            resize();
        }
        
        int mainBucketIndex = hash & bucketMask;
        int tableOffset = mainBucketIndex << BUCKET_SIZE_BITS;
        
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = table[tableOffset + i];
            
            if (slot == 0) {
                // Empty slot found: insert here
                int ptr = allocateEntry(key, namespace, null, hash);
                table[tableOffset + i] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                totalEntries++;
                checkResizeNeeded();
                return ptr;
            }
            
            if ((int)(slot >>> HASH_SHIFT) == hash) {
                int ptr = (int) slot;
                int base = (ptr - 1) * 2;
                Object entryKey = keyNs[base];
                if (entryKey != key && !entryKey.equals(key)) continue;

                Object entryNs = keyNs[base + 1];
                if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
                // Already exists: return existing ptr
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                return ptr;
            }
        }
        
        // Main bucket full, check/allocate extension
        long extLong = table[tableOffset + EXTENSION_SLOT_INDEX];
        int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
        int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
        
        if (extOffset == 0) {
            // Allocate new extension bucket
            extOffset = allocateExtensionBucket(mainBucketIndex);
            if (extOffset == 0) {
                // Extension limit reached, resize and retry
                resize();
                return put(key, namespace);
            }
            // Set extension pointer
            int shift = extIndex << 3;
            table[tableOffset + EXTENSION_SLOT_INDEX] = 
                (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
        }
        
        // search/insert in extension buckets
        int bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        int depth = 0;
        
        while (true) {
            int extTableOffset = (bucketIndex - bucketCount) << BUCKET_SIZE_BITS;
            
            // Search with Early Exit: slots are contiguous, empty = insert position
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = extensions[extTableOffset + i];
                
                if (slot == 0) {
                    // Empty slot found: insert here
                    int ptr = allocateEntry(key, namespace, null, hash);
                    extensions[extTableOffset + i] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                    if (l0CacheEnabled) {
                        l0Table.put(hash, ptr);
                    }
                    totalEntries++;
                    checkResizeNeeded();
                    return ptr;
                }
                
                if ((int)(slot >>> HASH_SHIFT) == hash) {
                    int ptr = (int) slot;
                    int base = (ptr - 1) * 2;
                    Object entryKey = keyNs[base];
                    if (entryKey != key && !entryKey.equals(key)) continue;

                    Object entryNs = keyNs[base + 1];
                    if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
                    // Already exists: return existing ptr
                    if (l0CacheEnabled) {
                        l0Table.put(hash, ptr);
                    }
                    return ptr;
                }
            }
            
            // Current bucket full, find/allocate extension bucket
            extLong = extensions[extTableOffset + EXTENSION_SLOT_INDEX];
            extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            
            if (extOffset == 0) {
                // Allocate new extension bucket
                extOffset = allocateExtensionBucket(mainBucketIndex);
                if (extOffset == 0) {
                    // Extension limit reached, resize and retry
                    resize();
                    return put(key, namespace);
                }
                // Set extension pointer
                int shift = extIndex << 3;
                extensions[extTableOffset + EXTENSION_SLOT_INDEX] = 
                    (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
            depth++;
            if (depth > maxExtensionDepth) {
                maxExtensionDepth = depth;
                if (maxExtensionDepth >= 2) {
                    LOG.info("⚠️ MainTable extension tree depth reached {}! Consider increasing bucket count or checking hash distribution.", maxExtensionDepth);
                }
            }
        }
    }

    /**
     * Removes an entry from the table and L0 cache.
     *
     * @param key the key object
     * @param namespace the namespace object
     * @return the removed state value (S), or null if not found
     */
    @Nullable
    public S remove(K key, N namespace) {
        int hash = computeHash(key, namespace);
        int mainBucketIndex = hash & bucketMask;
        int tableOffset = mainBucketIndex << BUCKET_SIZE_BITS;
        
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = table[tableOffset + i];
            if (slot == 0) break;
            if ((int)(slot >>> HASH_SHIFT) != hash) continue;
            
            int ptr = (int) slot;
            int base = (ptr - 1) * 2;
            Object entryKey = keyNs[base];
            if (entryKey != key && !entryKey.equals(key)) continue;

            Object entryNs = keyNs[base + 1];
            if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
            
            // Found: get state value before deletion
            S removedState = (S) states[ptr - 1];
            
            // Maintain contiguity by shifting forward from i+1
            int next = i + 1;
            while (next < SLOTS_PER_BUCKET && table[tableOffset + next] != 0) {
                table[tableOffset + next - 1] = table[tableOffset + next];
                next++;
            }
            // Clear the last occupied position
            table[tableOffset + next - 1] = 0;
            
            totalEntries--;
            if (l0CacheEnabled) {
                l0Table.remove(hash, ptr);
            }
            removeEntry(ptr);
            return removedState;
        }
        
        // Check if main bucket has extension
        long extLong = table[tableOffset + EXTENSION_SLOT_INDEX];
        int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
        int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
        if (extOffset == 0) {
            return null;  // Not found, no extension
        }
        
        int bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        while (true) {
            int extTableOffset = (bucketIndex - bucketCount) << BUCKET_SIZE_BITS;
            
            // Search with Early Exit and maintain contiguity on delete
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = extensions[extTableOffset + i];
                if (slot == 0) break;
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;
                
                int ptr = (int) slot;
                int base = (ptr - 1) * 2;
                Object entryKey = keyNs[base];
                if (entryKey != key && !entryKey.equals(key)) continue;

                Object entryNs = keyNs[base + 1];
                if ((entryNs != namespace && !entryNs.equals(namespace))) continue;
                
                // Found: get state value before deletion
                S removedState = (S) states[ptr - 1];
                
                // Maintain contiguity by shifting forward from i+1
                int next = i + 1;
                while (next < SLOTS_PER_BUCKET && extensions[extTableOffset + next] != 0) {
                    extensions[extTableOffset + next - 1] = extensions[extTableOffset + next];
                    next++;
                }
                // Clear the last occupied position
                extensions[extTableOffset + next - 1] = 0;
                
                totalEntries--;
                if (l0CacheEnabled) {
                    l0Table.remove(hash, ptr);
                }
                removeEntry(ptr);
                return removedState;
            }
            
            // Check next level extension
            extLong = extensions[extTableOffset + EXTENSION_SLOT_INDEX];
            extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return null;  // Not found
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    // ========== Entry Allocation (Flattened Storage) ==========

    /**
     * Allocates a new entry and returns its address (ptr).
     * Address is guaranteed to be > 0 (0 is reserved as NULL).
     */
    private int allocateEntry(@Nonnull K key, @Nonnull N namespace, @Nullable S state, int hash) {
        int entryIndex;
        
        // Prefer reusing freed slots (LIFO for cache efficiency)
        if (freeCount > 0) {
            entryIndex = freeList[--freeCount];
        } else {
            // Check if we need to expand entries array
            if (nextEntryIndex >= entryCapacity) {
                expandEntries();
            }
            entryIndex = nextEntryIndex++;
        }
        
        // Store K, N in keyNs array, S in states array
        int base = entryIndex * 2;
        keyNs[base] = key;
        keyNs[base + 1] = namespace;
        states[entryIndex] = state;
        hashes[entryIndex] = hash;
        
        // Return index + 1 to reserve 0 as NULL
        return entryIndex + 1;
    }

    /**
     * Removes an entry by its ptr (address) and adds slot to free list.
     */
    private void removeEntry(int ptr) {
        if (ptr <= 0) return;
        
        int entryIndex = ptr - 1;
        int base = entryIndex * 2;
        
        // Clear all references
        keyNs[base] = null;
        keyNs[base + 1] = null;
        states[entryIndex] = null;
        // hashes[entryIndex] not cleared (only used in rehash)
        
        // Add to free list (expand if necessary)
        if (freeCount >= freeList.length) {
            freeList = Arrays.copyOf(freeList, freeList.length * 2);
        }
        freeList[freeCount++] = entryIndex;
    }

    /**
     * Expands the keyNs and states arrays when more capacity is needed.
     */
    private void expandEntries() {
        int newCapacity = entryCapacity * 2;
        keyNs = Arrays.copyOf(keyNs, newCapacity * 2);
        states = Arrays.copyOf(states, newCapacity);
        hashes = Arrays.copyOf(hashes, newCapacity);
        entryCapacity = newCapacity;
    }

    // --- Iteration support ---

    /**
     * Iterates over all entries in the main table.
     *
     * @param visitor the visitor to call for each entry
     */
    public void forEachEntry(EntryVisitor visitor) {
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            visitBucketTree(bucketIndex, bucketIndex, visitor);
        }
    }

    private void visitBucketTree(int bucketIndex, int mainBucketIndex, EntryVisitor visitor) {
        // Determine if accessing main table or extension table
        boolean isExtension = (bucketIndex >= bucketCount);
        long[] currentTable = isExtension ? extensions : table;
        int tableOffset = isExtension ? (bucketIndex - bucketCount) << BUCKET_SIZE_BITS : bucketIndex << BUCKET_SIZE_BITS;
        
        // Visit 7 data slots
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = currentTable[tableOffset + i];
            if (slot != 0) {
                int hash = (int)(slot >>> HASH_SHIFT);
                int ptr = (int) slot;
                visitor.visit(ptr, hash);
            }
        }
        
        // Visit extension buckets if this main bucket has extension area
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            return;
        }
        
        long extLong = currentTable[tableOffset + EXTENSION_SLOT_INDEX];
        for (int i = 0; i < 8; i++) {
            int extOffset = (int)((extLong >>> (i << 3)) & EXT_PTR_MASK);
            if (extOffset != 0) {
                int extBucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
                visitBucketTree(extBucketIndex, mainBucketIndex, visitor);
            }
        }
    }

    // --- Extension bucket management ---

    private int allocateExtensionBucket(int mainBucketIndex) {
        // Check if single main bucket extension limit is reached
        if (extensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return 0;
        }
        
        // First extension of this main bucket: allocate an extension area from pool
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            if (extensionPoolUsed >= extensionPoolCapacity) {
                growExtensionPool();
            }
            extensionBucketBaseIndices[mainBucketIndex] = bucketCount + multiplyByExtAreaSize(extensionPoolUsed);
            extensionPoolUsed++;
        }
        
        int offset = ++extensionBucketCounts[mainBucketIndex];
        
        if (extensionBucketCounts[mainBucketIndex] > maxExtensionBucketsUsed) {
            maxExtensionBucketsUsed = extensionBucketCounts[mainBucketIndex];
        }
        
        return offset;
    }

    private void growExtensionPool() {
        // Each extension area can hold MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET (255) buckets
        int bucketsToAdd = multiplyByExtAreaSize(EXTENSION_POOL_GROW_SIZE);
        
        // Calculate new extension pool size needed
        int currentExtBuckets = multiplyByExtAreaSize(extensionPoolCapacity);
        int newExtBuckets = currentExtBuckets + bucketsToAdd;
        
        // Allocate or expand extensions array
        if (extensions == null) {
            extensions = new long[newExtBuckets << BUCKET_SIZE_BITS];
        } else if (newExtBuckets << BUCKET_SIZE_BITS > extensions.length) {
            extensions = Arrays.copyOf(extensions, newExtBuckets << BUCKET_SIZE_BITS);
        }
        
        extensionPoolCapacity += EXTENSION_POOL_GROW_SIZE;
    }

    // --- Public accessors ---

    public double getLoadFactor() {
        return (double) totalEntries / bucketCount;
    }

    public boolean needsResize() {
        return needsResize;
    }

    public int getMaxExtensionBucketsUsed() {
        return maxExtensionBucketsUsed;
    }

    public int getMaxExtensionDepth() {
        return maxExtensionDepth;
    }

    public TableStats getStats() {
        if (closed) {
            return new TableStats(0, 0, 0.0, 0, 0, 0, 0, 0, false);
        }
        return new TableStats(bucketCount, totalEntries, getLoadFactor(),
                             getMaxExtensionBucketsUsed(), getAllocatedExtensionBuckets(), 
                             extensionPoolUsed, getBucketsNeedingExtension(), maxExtensionDepth, needsResize);
    }

    /**
     * Gets L0 Table statistics for benchmark monitoring.
     * Returns null if L0 cache is not enabled.
     */
    @Nullable
    public L0Table.L0TableStats getL0Stats() {
        return l0Table != null ? l0Table.getStats() : null;
    }

    /**
     * Returns whether L0 cache is enabled.
     */
    public boolean isL0CacheEnabled() {
        return l0Table != null;
    }
    
    private int getBucketsNeedingExtension() {
        int count = 0;
        for (int i = 0; i < bucketCount; i++) {
            if (extensionBucketBaseIndices[i] > 0) {
                count++;
            }
        }
        return count;
    }

    private int getAllocatedExtensionBuckets() {
        int count = 0;
        for (int i = 0; i < bucketCount; i++) {
            if (extensionBucketBaseIndices[i] > 0) {
                count += extensionBucketCounts[i];
            }
        }
        return count;
    }

    // --- Resize operations ---

    /**
     * Resizes the main table by doubling the bucket count.
     */
    public void resize() {
        int newBucketCount = bucketCount * 2;
        int newBucketMask = newBucketCount - 1;
        
        // Create new flattened table
        long[] newTable = new long[newBucketCount << BUCKET_SIZE_BITS];
        long[] newExtensions = null;  // Allocated on demand
        
        int[] newExtensionBucketBaseIndices = new int[newBucketCount];
        int[] newExtensionBucketCounts = new int[newBucketCount];
        int newExtensionPoolCapacity = 0;
        int newExtensionPoolUsed = 0;
        int newMaxExtensionBucketsUsed = 0;
        
        // Rehash all entries (using hashes[] array)
        for (int entryIndex = 0; entryIndex < nextEntryIndex; entryIndex++) {
            int base = entryIndex * 2;
            if (keyNs[base] == null) continue;  // Deleted entry
            
            int hash = hashes[entryIndex];
            int ptr = entryIndex + 1;
            
            int bucketIndex = hash & newBucketMask;
            int mainBucketIndex = bucketIndex;
            
            // Insert into new table
            while (true) {
                // Determine which table to use
                boolean isExtension = (bucketIndex >= newBucketCount);
                long[] currentTable = isExtension ? newExtensions : newTable;
                int tableOffset = isExtension ? (bucketIndex - newBucketCount) << BUCKET_SIZE_BITS : bucketIndex << BUCKET_SIZE_BITS;
                
                int emptySlot = -1;
                
                // Find empty slot
                for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                    if (currentTable[tableOffset + i] == 0) {
                        emptySlot = i;
                        break;
                    }
                }
                
                if (emptySlot != -1) {
                    currentTable[tableOffset + emptySlot] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                    break;
                }
                
                // Need extension bucket
                long extLong = currentTable[tableOffset + EXTENSION_SLOT_INDEX];
                int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
                int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
                
                if (extOffset == 0) {
                    // Allocate new extension bucket
                    if (newExtensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
                        break;  // Should not happen
                    }
                    
                    if (newExtensionBucketBaseIndices[mainBucketIndex] == 0) {
                        if (newExtensionPoolUsed >= newExtensionPoolCapacity) {
                            newExtensionPoolCapacity += EXTENSION_POOL_GROW_SIZE;
                            int newExtBuckets = multiplyByExtAreaSize(newExtensionPoolCapacity);
                            if (newExtensions == null) {
                                newExtensions = new long[newExtBuckets << BUCKET_SIZE_BITS];
                            } else {
                                newExtensions = Arrays.copyOf(newExtensions, newExtBuckets << BUCKET_SIZE_BITS);
                            }
                        }
                        newExtensionBucketBaseIndices[mainBucketIndex] = 
                            newBucketCount + multiplyByExtAreaSize(newExtensionPoolUsed);
                        newExtensionPoolUsed++;
                    }
                    
                    extOffset = ++newExtensionBucketCounts[mainBucketIndex];
                    if (extOffset > newMaxExtensionBucketsUsed) {
                        newMaxExtensionBucketsUsed = extOffset;
                    }
                    
                    int shift = extIndex << 3;
                    currentTable[tableOffset + EXTENSION_SLOT_INDEX] = 
                        (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
                }
                
                bucketIndex = newExtensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
            }
        }
        
        // Switch to new table
        this.table = newTable;
        this.extensions = newExtensions;
        this.bucketCount = newBucketCount;
        this.bucketMask = newBucketMask;
        this.extensionBucketBaseIndices = newExtensionBucketBaseIndices;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.extensionPoolCapacity = newExtensionPoolCapacity;
        this.extensionPoolUsed = newExtensionPoolUsed;
        this.maxExtensionBucketsUsed = newMaxExtensionBucketsUsed;
        this.needsResize = false;
    }

    private void checkResizeNeeded() {
        if (getLoadFactor() >= loadFactorThreshold || maxExtensionBucketsUsed >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            needsResize = true;
        }
    }

    /**
     * Multiplies x by 255 using bit shifts: x * 255 = x * (256-1) = (x << 8) - x
     */
    private static int multiplyByExtAreaSize(int x) {
        return (x << EXTENSION_AREA_SIZE_BITS) - x;
    }

    @FunctionalInterface
    public interface EntryVisitor {
        void visit(int ptr, int hash);
    }

    public static class TableStats {
        public final int bucketCount;
        public final int totalEntries;
        public final double loadFactor;
        public final int maxExtensionBuckets;
        public final int allocatedExtensionBuckets;
        public final int extensionAreasUsed;
        public final int bucketsNeedingExtension;
        public final int maxExtensionDepth;
        public final boolean needsResize;

        public TableStats(int bucketCount, int totalEntries, double loadFactor,
                         int maxExtensionBuckets, int allocatedExtensionBuckets, 
                         int extensionAreasUsed, int bucketsNeedingExtension, int maxExtensionDepth, boolean needsResize) {
            this.bucketCount = bucketCount;
            this.totalEntries = totalEntries;
            this.loadFactor = loadFactor;
            this.maxExtensionBuckets = maxExtensionBuckets;
            this.allocatedExtensionBuckets = allocatedExtensionBuckets;
            this.extensionAreasUsed = extensionAreasUsed;
            this.bucketsNeedingExtension = bucketsNeedingExtension;
            this.maxExtensionDepth = maxExtensionDepth;
            this.needsResize = needsResize;
        }

        @Override
        public String toString() {
            return String.format("MainTable[buckets=%d, entries=%d, load=%.2f, maxExt=%d, maxDepth=%d, extAreas=%d, bucketsNeedExt=%d, needsResize=%s]",
                bucketCount, totalEntries, loadFactor, maxExtensionBuckets, maxExtensionDepth, extensionAreasUsed, bucketsNeedingExtension, needsResize);
        }
    }

    /**
     * Returns the number of entries in the table.
     *
     * @return the number of entries
     */
    public int size() {
        return totalEntries;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;  // Idempotent: safe to call multiple times
        }
        closed = true;
        
        // Close L0 table if present
        if (l0Table != null) {
            l0Table.close();
        }
        
        // Clear entry storage
        if (keyNs != null) {
            Arrays.fill(keyNs, null);
            keyNs = null;
        }
        if (states != null) {
            Arrays.fill(states, null);
            states = null;
        }
        hashes = null;
        freeList = null;
        
        // Clear bucket tables
        table = null;
        extensions = null;
        extensionBucketBaseIndices = null;
        extensionBucketCounts = null;
    }
}
