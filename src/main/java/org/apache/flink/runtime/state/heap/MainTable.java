package org.apache.flink.runtime.state.heap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    // Chunk configuration
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

    // Heap storage: chunked long arrays
    private long[][] chunks;
    private int bucketCount;
    
    // Extension bucket management
    private int[] extensionBucketCounts;       // Number of extension buckets per main bucket
    private int[] extensionBucketBaseIndices;  // Base bucket index for each main bucket's extension area
    private int extensionPoolCapacity;         // Total extension areas available
    private int extensionPoolUsed;             // Extension areas allocated to main buckets
    
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    private int maxExtensionBucketsUsed = 0;
    private int maxExtensionDepth = 0;  // True tree depth (traversal count)

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
    }

    /**
     * Gets an entry from the table. Checks L0 cache first if present.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the HeapStateEntry if found, null otherwise
     */
    @SuppressWarnings("null")
    public HeapStateEntry<K, N, S> get(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
        // Check L0 cache first (JIT can eliminate this branch if l0CacheEnabled is false)
        if (l0CacheEnabled) {
            HeapStateEntry<K, N, S> entry = l0Table.get(hash, key, namespace, store);
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
            
            // Search 7 data slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) continue;  // Empty slot
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;  // Hash mismatch

                int ptr = (int) slot;
                int idx = ptr - 1;
                HeapStateEntry<K, N, S> entry = store.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
                // Entry is guaranteed non-null when slot != 0
                if (entry.key.equals(key) 
                        && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                    // Found in MainTable, update L0 cache
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
     * Inserts or finds an entry. If new, creates an Entry in store with state=null.
     * Automatically resizes the table if needed and updates L0 cache.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the HeapStateEntry (existing or newly created with state=null)
     */
    @SuppressWarnings("null")
    public HeapStateEntry<K, N, S> put(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
        // Auto-resize before insertion if needed
        if (needsResize) {
            resize(store);
        }
        
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        int depth = 0;  // Track extension depth for this lookup
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
            long[] chunk = chunks[chunkIndex];
            int emptySlot = -1;
            
            // Search 7 data slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                
                if (slot == 0) {
                    if (emptySlot == -1) emptySlot = i;
                    continue;
                }
                
                if ((int)(slot >>> HASH_SHIFT) == hash) {
                    int ptr = (int) slot;
                    int idx = ptr - 1;
                    HeapStateEntry<K, N, S> entry = store.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
                    // Entry is guaranteed non-null when slot != 0
                    if (entry.key.equals(key) 
                            && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                        // Existing entry found
                        return entry;
                    }
                }
            }
            
            if (emptySlot != -1) {
                // New entry: allocate in store with pre-computed hash
                int ptr = (int) store.allocate(key, namespace, null, hash);
                chunk[offset + emptySlot] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                
                // Get the newly created entry
                int idx = ptr - 1;
                HeapStateEntry<K, N, S> entry = store.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
                
                // Update L0 cache
                if (l0CacheEnabled) {
                    l0Table.put(hash, ptr);
                }
                
                totalEntries++;
                checkResizeNeeded();
                return entry;
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
                    resize(store);
                    return put(hash, key, namespace, store);
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
     * @param store the HeapEntryStore containing the entries
     * @return the removed HeapStateEntry if found, null otherwise
     */
    @SuppressWarnings("null")
    public HeapStateEntry<K, N, S> remove(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) << BUCKET_SIZE_BITS;
            long[] chunk = chunks[chunkIndex];
            
            // Search 7 data slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) continue;
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;
                
                int ptr = (int) slot;
                int idx = ptr - 1;
                HeapStateEntry<K, N, S> entry = store.chunks[idx >> HeapEntryStore.CHUNK_BITS][idx & HeapEntryStore.CHUNK_MASK];
                // Entry is guaranteed non-null when slot != 0
                if (entry.key.equals(key) 
                        && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                    // Found: clear slot
                    chunk[offset + i] = 0;
                    totalEntries--;
                    
                    // Remove from L0 cache
                    if (l0CacheEnabled) {
                        l0Table.remove(hash, ptr);
                    }
                    
                    // Remove from store
                    store.remove(ptr);
                    
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
    public void resize(HeapEntryStore<K, N, S> entryStore) {
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
        
        // Migrate all entries from HeapEntryStore
        long maxAddr = entryStore.getMaxAddress();
        for (int index = 0; index < maxAddr; index++) {
            HeapStateEntry<K, N, S> entry = entryStore.getByIndex(index);
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
        // Close L0 table if present
        if (l0Table != null) {
            l0Table.close();
        }
        // Heap arrays are automatically managed by GC, no manual cleanup needed
        chunks = null;
        extensionBucketBaseIndices = null;
        extensionBucketCounts = null;
    }
}
