package org.apache.flink.runtime.state.heap;

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
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class MainTable<K, N, S> implements AutoCloseable {

    // Bucket layout constants
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
    
    // Extension pointer: 4 x 8-bit offsets in low 32 bits of extension long
    private static final int EXT_PTR_MASK = 0xFF;
    private static final int MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET = 255;
    
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

    // Cached location for setSlot optimization
    private int lastFoundChunkIndex = -1;
    private int lastFoundSlotOffset = -1;

    public MainTable(int bucketCountPow2) {
        this(bucketCountPow2, DEFAULT_LOAD_FACTOR_THRESHOLD);
    }

    public MainTable(int bucketCountPow2, double loadFactorThreshold) {
        this.loadFactorThreshold = loadFactorThreshold;
        this.bucketCount = 1 << bucketCountPow2;
        
        // Allocate main bucket chunks
        // Each chunk is allocated with full CHUNK_SIZE capacity to accommodate extension buckets
        int requiredChunks = (bucketCount + CHUNK_SIZE - 1) >>> CHUNK_SIZE_BITS;
        if (requiredChunks == 0) requiredChunks = 1;
        
        this.chunks = new long[requiredChunks][];
        for (int i = 0; i < requiredChunks; i++) {
            // Always allocate full chunk size to leave room for extension buckets
            this.chunks[i] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
            // Arrays are zero-initialized by JVM, no need to clear
        }
        
        // Initialize management arrays
        this.extensionBucketBaseIndices = new int[bucketCount];
        this.extensionBucketCounts = new int[bucketCount];
        this.extensionPoolCapacity = 0;
        this.extensionPoolUsed = 0;
    }

    /**
     * Gets an entry from the main table using object comparison.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the entry pointer if found, 0 otherwise
     */
    public int get(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;
            long[] chunk = chunks[chunkIndex];
            
            // Search 7 data slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) continue;  // Empty slot
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;  // Hash mismatch
                int ptr = (int) slot;
                if (store.matches(ptr, key, namespace)) {
                    lastFoundChunkIndex = chunkIndex;
                    lastFoundSlotOffset = offset + i;
                    return ptr;
                }
            }
            
            // Check extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = hash & 0x3;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return 0;  // Not found
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    /**
     * Inserts or updates an entry using object comparison.
     *
     * @param hash the pre-computed hash value
     * @param ptr the HeapEntryStore pointer of the entry
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return 0 for new entry, positive for existing entry pointer, -1 for full (needs resize)
     */
    public int put(int hash, int ptr, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;
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
                    int slotPtr = (int) slot;
                    if (store.matches(slotPtr, key, namespace)) {
                        // Update existing entry
                        lastFoundChunkIndex = chunkIndex;
                        lastFoundSlotOffset = offset + i;
                        if (ptr > 0) {
                            chunk[offset + i] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                        }
                        return slotPtr;
                    }
                }
            }
            
            if (emptySlot != -1) {
                // Found an empty slot - just record location, don't write yet
                lastFoundChunkIndex = chunkIndex;
                lastFoundSlotOffset = offset + emptySlot;
                if (ptr > 0) {
                    // Insert into empty slot only if we have a valid pointer
                    chunk[offset + emptySlot] = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
                }
                // New entry: increment count and check resize
                totalEntries++;
                checkResizeNeeded();
                return 0;
            }
            
            // Current bucket full, find/allocate extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = hash & 0x3;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            
            if (extOffset == 0) {
                // Allocate new extension bucket
                extOffset = allocateExtensionBucket(mainBucketIndex);
                if (extOffset == 0) {
                    needsResize = true;
                    return -1;
                }
                // Set extension pointer
                int shift = extIndex << 3;
                chunk[offset + EXTENSION_SLOT_INDEX] = 
                    (extLong & ~((long) EXT_PTR_MASK << shift)) | ((long) extOffset << shift);
            }
            
            bucketIndex = extensionBucketBaseIndices[mainBucketIndex] + extOffset - 1;
        }
    }

    /**
     * Sets the slot at the last found location with the given hash and pointer.
     * Must be called after a successful put() that returned 0 (new entry) to commit the entry.
     * Caller must ensure this is called correctly after put() returns 0.
     *
     * @param hash the hash value for the entry
     * @param ptr the pointer to the entry in HeapEntryStore
     */
    public void setSlot(int hash, int ptr) {
        chunks[lastFoundChunkIndex][lastFoundSlotOffset] = 
            ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);
    }

    /**
     * Removes an entry from the main table using object comparison.
     *
     * @param hash the pre-computed hash value
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the removed entry pointer if found, 0 otherwise
     */
    public int remove(int hash, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = hash & (bucketCount - 1);
        int mainBucketIndex = bucketIndex;
        
        while (true) {
            int chunkIndex = bucketIndex >>> CHUNK_SIZE_BITS;
            int offset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;
            long[] chunk = chunks[chunkIndex];
            
            // Search 7 data slots
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = chunk[offset + i];
                if (slot == 0) continue;
                if ((int)(slot >>> HASH_SHIFT) != hash) continue;
                int ptr = (int) slot;
                if (store.matches(ptr, key, namespace)) {
                    chunk[offset + i] = 0;  // Clear slot
                    totalEntries--;
                    return ptr;
                }
            }
            
            // Check extension bucket
            long extLong = chunk[offset + EXTENSION_SLOT_INDEX];
            int extIndex = hash & 0x3;
            int extOffset = (int)((extLong >>> (extIndex << 3)) & EXT_PTR_MASK);
            if (extOffset == 0) {
                return 0;  // Not found
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
        int offset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;
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
        for (int i = 0; i < 4; i++) {
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
                growExtensionPool(EXTENSION_POOL_GROW_SIZE);
            }
            extensionBucketBaseIndices[mainBucketIndex] = bucketCount + extensionPoolUsed * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            extensionPoolUsed++;
        }
        
        int offset = ++extensionBucketCounts[mainBucketIndex];
        
        if (extensionBucketCounts[mainBucketIndex] > maxExtensionBucketsUsed) {
            maxExtensionBucketsUsed = extensionBucketCounts[mainBucketIndex];
        }
        
        return offset;
    }

    private void growExtensionPool(int areasToAdd) {
        // Each extension area can hold MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET (255) buckets
        int bucketsToAdd = areasToAdd * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
        
        // Calculate how many new chunks we need
        int currentTotalBuckets = bucketCount + extensionPoolCapacity * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
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
        
        extensionPoolCapacity += areasToAdd;
    }

    // --- Public accessors ---

    public double getLoadFactor() {
        return bucketCount == 0 ? 0.0 : (double) totalEntries / bucketCount;
    }

    public boolean needsResize() {
        return needsResize;
    }

    public int getMaxExtensionBucketsUsed() {
        return maxExtensionBucketsUsed;
    }

    public TableStats getStats() {
        return new TableStats(bucketCount, totalEntries, getLoadFactor(),
                             getMaxExtensionBucketsUsed(), getAllocatedExtensionBuckets(), 
                             extensionPoolUsed, getBucketsNeedingExtension(), needsResize);
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

    public void tryResize(HeapEntryStore<K, N, S> entryStore) {
        if (!needsResize) return;
        resize(entryStore);
    }

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
                int offset = (bucketIndex & CHUNK_MASK) * BUCKET_SIZE_LONGS;
                
                // Ensure chunk exists
                if (chunkIndex >= newChunks.length) {
                    newChunks = Arrays.copyOf(newChunks, Math.max(newChunks.length * 2, chunkIndex + 1));
                }
                if (newChunks[chunkIndex] == null) {
                    newChunks[chunkIndex] = new long[CHUNK_SIZE * BUCKET_SIZE_LONGS];
                }
                
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
                int extIndex = hash & 0x3;
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
                            newBucketCount + newExtensionPoolUsed * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
                        newExtensionPoolUsed++;
                    }
                    
                    extOffset = ++newExtensionBucketCounts[mainBucketIndex];
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
        this.needsResize = false;
        
        // Update max extension buckets used
        int max = 0;
        for (int c : extensionBucketCounts) {
            if (c > max) max = c;
        }
        maxExtensionBucketsUsed = max;
    }

    private void checkResizeNeeded() {
        if (getLoadFactor() >= loadFactorThreshold || maxExtensionBucketsUsed >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            needsResize = true;
        }
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
        public final boolean needsResize;

        public TableStats(int bucketCount, int totalEntries, double loadFactor,
                         int maxExtensionBuckets, int allocatedExtensionBuckets, 
                         int extensionAreasUsed, int bucketsNeedingExtension, boolean needsResize) {
            this.bucketCount = bucketCount;
            this.totalEntries = totalEntries;
            this.loadFactor = loadFactor;
            this.maxExtensionBuckets = maxExtensionBuckets;
            this.allocatedExtensionBuckets = allocatedExtensionBuckets;
            this.extensionAreasUsed = extensionAreasUsed;
            this.bucketsNeedingExtension = bucketsNeedingExtension;
            this.needsResize = needsResize;
        }

        @Override
        public String toString() {
            return String.format("MainTable[buckets=%d, entries=%d, load=%.2f, maxExt=%d, extAreas=%d, bucketsNeedExt=%d, needsResize=%s]",
                bucketCount, totalEntries, loadFactor, maxExtensionBuckets, extensionAreasUsed, bucketsNeedingExtension, needsResize);
        }
    }

    @Override
    public void close() throws Exception {
        // Heap arrays are automatically managed by GC, no manual cleanup needed
        chunks = null;
        extensionBucketBaseIndices = null;
        extensionBucketCounts = null;
    }
}
