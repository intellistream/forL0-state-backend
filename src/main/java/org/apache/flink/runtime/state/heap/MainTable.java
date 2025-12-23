package org.apache.flink.runtime.state.heap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Main Table implementation for ForL0 State Backend.
 * 
 * <p>This is the heap-based version using chunked long[][] arrays for O(1) access.
 * Each slot is a single long: [Hash(32b) | Ptr(32b)].
 * 
 * <p>64-byte aligned buckets with 7 data slots + 1 extension long.
 * Supports tree-like expansion and global resize.
 * 
 * <p>Entry storage is integrated directly into MainTable (previously HeapEntryStore),
 * avoiding indirect access overhead.
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
    private static final int BUCKET_SIZE_LONGS = 8;      // 64 bytes = 8 longs
    private static final int SLOTS_PER_BUCKET = 7;       // 7 data slots
    private static final int EXTENSION_SLOT_INDEX = 7;   // 8th long for extension pointers
    
    // Chunk configuration for bucket storage
    private static final int CHUNK_SIZE_BITS = 16;       // 65536 buckets per chunk
    private static final int CHUNK_SIZE = 1 << CHUNK_SIZE_BITS;
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;
    
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

    // ========== Entry Storage (integrated from HeapEntryStore) ==========
    
    /** Entry chunk configuration: 65536 entries per chunk. */
    static final int ENTRY_CHUNK_BITS = 16;
    static final int ENTRY_CHUNK_SIZE = 1 << ENTRY_CHUNK_BITS;
    static final int ENTRY_CHUNK_MASK = ENTRY_CHUNK_SIZE - 1;
    private static final int INITIAL_FREE_LIST_SIZE = 1024;

    /** Chunked entry storage: entryChunks[chunkIndex][slotIndex]. */
    HeapStateEntry<K, N, S>[][] entryChunks;
    
    /** Current number of allocated entry chunks. */
    private int entryChunkCount;
    
    /** Next allocation index for entries (max valid ptr = nextEntryAllocIndex). */
    int nextEntryAllocIndex;
    
    /** Free list for deleted entry slots. */
    private int[] freeList;
    private int freeCount;

    // ========== Bucket Index Storage ==========
    
    // Heap storage: chunked long arrays for bucket indices
    private long[][] chunks;
    private int bucketCount;
    
    // Extension bucket management
    private int[] extensionBucketCounts;       // Number of extension buckets per main bucket
    private int[] extensionBucketBaseIndices;  // Base bucket index for each main bucket's extension area
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

    // ========== Entry Chunk Cache (reduce pointer chasing) ==========
    
    /** Cached entry chunk to reduce entryChunks[i] indirection.
     *  Exploits spatial locality: entries in same bucket often share same chunk. */
    private HeapStateEntry<K, N, S>[] cachedEntryChunk = null;
    private int cachedEntryChunkIndex = -1;

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
    @SuppressWarnings("unchecked")
    public MainTable(double loadFactorThreshold, @Nullable L0Table<K, N, S> l0Table) {
        this.loadFactorThreshold = loadFactorThreshold;
        this.l0Table = l0Table;
        this.l0CacheEnabled = (l0Table != null);  // Final for JIT optimization
        
        // Fixed initial size: one full chunk (65536 buckets = 4MB)
        this.bucketCount = CHUNK_SIZE;
        
        // Allocate exactly one chunk (4MB) initially
        this.chunks = new long[1][];
        this.chunks[0] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
        
        // Initialize management arrays
        this.extensionBucketBaseIndices = new int[bucketCount];
        this.extensionBucketCounts = new int[bucketCount];
        this.extensionPoolCapacity = 0;
        this.extensionPoolUsed = 0;
        
        // Initialize entry storage (integrated from HeapEntryStore)
        this.entryChunks = (HeapStateEntry<K, N, S>[][]) new HeapStateEntry[1][];
        this.entryChunks[0] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[ENTRY_CHUNK_SIZE];
        this.entryChunkCount = 1;
        this.nextEntryAllocIndex = 0;
        this.freeList = new int[INITIAL_FREE_LIST_SIZE];
        this.freeCount = 0;
    }

    /**
     * Fast entry access with chunk caching.
     * Reduces one level of indirection by caching the most recently accessed entry chunk.
     * 
     * @param ptr the entry pointer (1-based)
     * @return the HeapStateEntry
     */
    private HeapStateEntry<K, N, S> getEntry(int ptr) {
        int idx = ptr - 1;
        int chunkIdx = idx >> ENTRY_CHUNK_BITS;
        
        // Fast path: cache hit
        if (chunkIdx == cachedEntryChunkIndex) {
            return cachedEntryChunk[idx & ENTRY_CHUNK_MASK];
        }
        
        // Slow path: cache miss, update cache
        cachedEntryChunk = entryChunks[chunkIdx];
        cachedEntryChunkIndex = chunkIdx;
        return cachedEntryChunk[idx & ENTRY_CHUNK_MASK];
    }

    /**
     * Gets an entry from the table. Checks L0 cache first if present.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @return the HeapStateEntry if found, null otherwise
     */
    public HeapStateEntry<K, N, S> get(int hash, K key, N namespace) {
        // Check L0 cache first (JIT can eliminate this branch if l0CacheEnabled is false)
        if (l0CacheEnabled) {
            HeapStateEntry<K, N, S> entry = l0Table.get(hash, key, namespace, entryChunks);
            if (entry != null) {
                return entry;
            }
        }
        
        // Search in MainTable
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
            long[] chunk = chunks[chunkIndex];
            
            // Search slots with Early Exit: slots are contiguous, empty = end
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) break;  // Early exit: no more entries in this bucket
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;  // Hash mismatch

                int ptr = (int) slot;
                HeapStateEntry<K, N, S> entry = getEntry(ptr);
                if (entry.key.equals(key) 
                        && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                    if (l0CacheEnabled) {
                        l0Table.put(hash, ptr);
                    }
                    return entry;
                }
            }
            
            // Check extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return null;  // Not found
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    /**
     * Inserts or finds an entry. If new, creates an Entry with state=null.
     * Automatically resizes the table if needed and updates L0 cache.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @return the HeapStateEntry (existing or newly created with state=null)
     */
    public HeapStateEntry<K, N, S> put(int hash, K key, N namespace) {
        // Auto-resize before insertion if needed
        if (needsResize) {
            resize();
        }
        
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        int depth = 0;  // Track extension depth for this lookup
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
            long[] chunk = chunks[chunkIndex];
            // Search with Early Exit: slots are contiguous, empty = insert position
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                
                if (slot == 0) {
                    // Empty slot found: insert here (maintains contiguity)
                    int ptr = allocateEntry(key, namespace, null, hash);
                    chunk[offset + i] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                    HeapStateEntry<K, N, S> entry = getEntry(ptr);
                    if (l0CacheEnabled) {
                        l0Table.put(hash, ptr);
                    }
                    totalEntries++;
                    checkResizeNeeded();
                    return entry;
                }
                
                if ((int)(slot >>> HASH_SHIFT) == hash) {
                    int ptr = (int) slot;
                    HeapStateEntry<K, N, S> entry = getEntry(ptr);
                    if (entry.key.equals(key) 
                            && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                        return entry;
                    }
                }
            }
            
            // Current bucket full, find/allocate extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            
            if (extOffset == 0) {
                // Allocate new extension bucket
                extOffset = allocateExtensionBucket(mainBucketIndex);
                if (extOffset == 0) {
                    // Extension limit reached, resize and retry
                    resize();
                    return put(hash, key, namespace);
                }
                // Set extension pointer
                int shift = extIndex << 3;
                chunk[offset + EXTENSION_SLOT_INDEX] = 
                    (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
            // depth tracks extension level: 1=first extension bucket, 2=second, etc.
            // Increment BEFORE entering extension bucket so we count the level correctly
            depth++;
            if (depth > maxExtensionDepth) {
                maxExtensionDepth = depth;
                if (maxExtensionDepth >= 3) {
                    LOG.info("⚠️ MainTable extension tree depth reached {}! Consider increasing bucket count or checking hash distribution.", maxExtensionDepth);
                }
            }
        }
    }

    /**
     * Removes an entry from the table and L0 cache.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @return the removed HeapStateEntry if found, null otherwise
     */
    public HeapStateEntry<K, N, S> remove(int hash, K key, N namespace) {
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
            long[] chunk = chunks[chunkIndex];
            
            // Search with Early Exit and maintain contiguity on delete
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) break;  // Early exit: no more entries
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;
                
                int ptr = (int) slot;
                HeapStateEntry<K, N, S> entry = getEntry(ptr);
                if (entry.key.equals(key) 
                        && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                    // Found: maintain contiguity by shifting forward from i+1
                    // Since average load is ~1.5, forward search is more efficient
                    int next = i + 1;
                    while (next < SLOTS_PER_BUCKET && chunk[offset + next] != 0) {
                        chunk[offset + next - 1] = chunk[offset + next];
                        next++;
                    }
                    // Clear the last occupied position
                    chunk[offset + next - 1] = 0;
                    
                    totalEntries--;
                    if (l0CacheEnabled) {
                        l0Table.remove(hash, ptr);
                    }
                    removeEntry(ptr);
                    return entry;
                }
            }
            
            // Check extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return null;  // Not found
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    // ========== Entry Allocation (integrated from HeapEntryStore) ==========

    /**
     * Allocates a new entry and returns its address (ptr).
     * Address is guaranteed to be > 0 (0 is reserved as NULL).
     */
    private int allocateEntry(@Nonnull K key, @Nonnull N namespace, @Nullable S state, int hash) {
        int index;
        
        // Prefer reusing freed slots (LIFO for cache efficiency)
        if (freeCount > 0) {
            index = freeList[--freeCount];
        } else {
            // Check if we need to expand entry chunks
            if (nextEntryAllocIndex >= entryChunkCount * ENTRY_CHUNK_SIZE) {
                expandEntryChunks();
            }
            index = nextEntryAllocIndex++;
        }
        
        // Create and store the entry
        HeapStateEntry<K, N, S> entry = new HeapStateEntry<>(key, namespace, state, hash);
        entryChunks[index >> ENTRY_CHUNK_BITS][index & ENTRY_CHUNK_MASK] = entry;
        
        // Return index + 1 to reserve 0 as NULL
        return index + 1;
    }

    /**
     * Removes an entry by its ptr (address) and adds slot to free list.
     */
    private void removeEntry(int ptr) {
        if (ptr <= 0) return;
        
        int index = ptr - 1;
        int chunkIndex = index >> ENTRY_CHUNK_BITS;
        
        if (chunkIndex < entryChunkCount && entryChunks[chunkIndex][index & ENTRY_CHUNK_MASK] != null) {
            entryChunks[chunkIndex][index & ENTRY_CHUNK_MASK] = null;
            
            // Add to free list (expand if necessary)
            if (freeCount >= freeList.length) {
                freeList = Arrays.copyOf(freeList, freeList.length * 2);
            }
            freeList[freeCount++] = index;
        }
    }

    /**
     * Expands the entry chunk array when more capacity is needed.
     */
    @SuppressWarnings("unchecked")
    private void expandEntryChunks() {
        int newChunkCount = entryChunkCount + 1;
        
        if (newChunkCount > entryChunks.length) {
            int newLength = Math.max(entryChunks.length * 2, newChunkCount);
            entryChunks = Arrays.copyOf(entryChunks, newLength);
        }
        
        entryChunks[entryChunkCount] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[ENTRY_CHUNK_SIZE];
        entryChunkCount = newChunkCount;
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
        int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
        int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
        long[] chunk = chunks[chunkIndex];
        
        // Visit 7 data slots
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = chunk[offset + i];
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
        
        long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
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
        
        // Calculate how many new chunks we need
        int currentTotalBuckets = bucketCount + multiplyByExtAreaSize(extensionPoolCapacity);
        int newTotalBuckets = currentTotalBuckets + bucketsToAdd;
        int currentChunks = (currentTotalBuckets + CHUNK_SIZE - 1) >>> CHUNK_SIZE_BITS;
        int requiredChunks = (newTotalBuckets + CHUNK_SIZE - 1) >>> CHUNK_SIZE_BITS;
        
        if (requiredChunks > chunks.length) {
            chunks = Arrays.copyOf(chunks, Math.max(chunks.length * 2, requiredChunks));
        }
        
        // Allocate new chunks as needed
        for (int i = currentChunks; i < requiredChunks; i++) {
            if (chunks[i] == null) {
                chunks[i] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
            }
        }
        
        extensionPoolCapacity += MainTable.EXTENSION_POOL_GROW_SIZE;
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
        
        // Create new table structure
        int newRequiredChunks = (newBucketCount + CHUNK_SIZE - 1) >>> CHUNK_SIZE_BITS;
        if (newRequiredChunks == 0) newRequiredChunks = 1;
        
        long[][] newChunks = new long[newRequiredChunks][];
        for (int i = 0; i < newRequiredChunks; i++) {
            // Always allocate full chunk size to leave room for extension buckets
            newChunks[i] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
        }
        
        int[] newExtensionBucketBaseIndices = new int[newBucketCount];
        int[] newExtensionBucketCounts = new int[newBucketCount];
        int newExtensionPoolCapacity = 0;
        int newExtensionPoolUsed = 0;
        int newMaxExtensionBucketsUsed = 0;  // Track max during migration
        
        // Migrate all entries from entryChunks
        for (int index = 0; index < nextEntryAllocIndex; index++) {
            HeapStateEntry<K, N, S> entry = entryChunks[index >> ENTRY_CHUNK_BITS][index & ENTRY_CHUNK_MASK];
            if (entry == null) continue;
            
            int hash = entry.hash;
            int ptr = index + 1;
            
            int bucketIndex = hash & (newBucketCount - 1);
            int mainBucketIndex = bucketIndex;
            
            // Put into new table (simplified inline logic)
            while (true) {
                int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
                int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
                long[] chunk = newChunks[chunkIndex];
                int emptySlot = -1;
                
                for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                    if (chunk[offset + i] == 0) {
                        emptySlot = i;
                        break;
                    }
                }
                
                if (emptySlot != -1) {
                    chunk[offset + emptySlot] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                    break;
                }
                
                // Allocate extension bucket
                long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
                int extIndex = (hash >>> EXT_INDEX_SHIFT) & EXT_INDEX_MASK;
                int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
                
                if (extOffset == 0) {
                    // Allocate new extension bucket for new table
                    if (newExtensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
                        break;  // Should not happen in normal operation
                    }
                    
                    if (newExtensionBucketBaseIndices[mainBucketIndex] == 0) {
                        if (newExtensionPoolUsed >= newExtensionPoolCapacity) {
                            newExtensionPoolCapacity += EXTENSION_POOL_GROW_SIZE;
                        }
                        newExtensionBucketBaseIndices[mainBucketIndex] = 
                            newBucketCount + multiplyByExtAreaSize(newExtensionPoolUsed);
                        newExtensionPoolUsed++;
                    }
                    
                    extOffset = ++newExtensionBucketCounts[mainBucketIndex];
                    // Track max extension buckets during migration
                    if (extOffset > newMaxExtensionBucketsUsed) {
                        newMaxExtensionBucketsUsed = extOffset;
                    }
                    
                    // Ensure chunk exists for extension bucket
                    int extChunkIndex = (newExtensionBucketBaseIndices[mainBucketIndex] + extOffset - 1) >>> CHUNK_SIZE_BITS;
                    if (extChunkIndex >= newChunks.length) {
                        newChunks = Arrays.copyOf(newChunks, Math.max(newChunks.length * 2, extChunkIndex + 1));
                    }
                    if (newChunks[extChunkIndex] == null) {
                        newChunks[extChunkIndex] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
                    }
                    
                    int shift = extIndex << 3;
                    chunk[offset + EXTENSION_SLOT_INDEX] = 
                        (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
                }
                
                bucketIndex = newExtensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
            }
        }
        
        // Switch to new table
        this.chunks = newChunks;
        this.bucketCount = newBucketCount;
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
        if (entryChunks != null) {
            for (int i = 0; i < entryChunkCount; i++) {
                if (entryChunks[i] != null) {
                    Arrays.fill(entryChunks[i], null);
                    entryChunks[i] = null;
                }
            }
            entryChunks = null;
        }
        freeList = null;
        // Heap arrays are automatically managed by GC, no manual cleanup needed
        chunks = null;
        extensionBucketBaseIndices = null;
        extensionBucketCounts = null;
    }
}
