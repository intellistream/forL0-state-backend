package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main Table implementation for ForL0 State Backend.
 * 
 * <p>This is the heap object store version that uses object comparison instead of
 * byte comparison. The Pointer field now stores the array index in HeapEntryStore
 * instead of off-heap memory address.
 * 
 * <p>64-byte aligned buckets with 6 slots + 4 extension pointers.
 * Supports tree-like expansion and global resize.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class MainTable<K, N, S> implements AutoCloseable {

    private static final int BUCKET_SIZE = 64;
    private static final int SLOTS_PER_BUCKET = 6;
    private static final int SLOT_SIZE = 10;
    private static final int EXTENSION_POINTERS = 4;
    private static final int SLOT_TAG_OFFSET = 0;
    private static final int SLOT_POINTER_OFFSET = 2;
    private static final int EXTENSION_POINTERS_OFFSET = 60;
    private static final double DEFAULT_LOAD_FACTOR_THRESHOLD = 1.5;
    private static final int MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET = 255;
    private static final byte NULL_BUCKET_ID = 0;
    
    // Extension pool configuration
    private static final int EXTENSION_AREA_SIZE = MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET * BUCKET_SIZE;  // 16320 bytes per area
    private static final int POOL_GROW_AREAS = 32;  // Allocate 32 extension areas at a time

    private final MemoryManagerAllocator allocator;
    private int bucketCount;
    
    // Segment storage: array for O(1) access
    private MemorySegment[] memorySegments;
    private int memorySegmentCount;
    private int segmentSize;  // Cached segment size to avoid repeated .size() calls
    
    // Track all allocations separately for proper release
    private final List<List<MemorySegment>> allAllocations = new ArrayList<>();
    
    // Extension bucket pool management
    private int extensionPoolCapacity;  // Number of extension areas available in pool
    private int extensionPoolUsed;      // Number of extension areas allocated to main buckets
    
    private int[] extensionBucketCounts;
    private int[] extensionBucketBaseIndices;
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    private int maxExtensionBucketsUsed = 0;

    private MemorySegment lastFoundSegment = null;
    private int lastFountSlotOffset = -1;

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this(allocator, bucketCountPow2, DEFAULT_LOAD_FACTOR_THRESHOLD);
    }

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2, double loadFactorThreshold) {
        this.allocator = allocator;
        this.loadFactorThreshold = loadFactorThreshold;
        this.bucketCount = 1 << bucketCountPow2;
        
        try {
            // 只分配主桶内存
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            List<MemorySegment> initialAllocation = allocator.allocate((int) totalSize);
            allAllocations.add(initialAllocation);  // 跟踪原始分配
            
            // 使用数组存储，O(1) 访问
            this.memorySegments = initialAllocation.toArray(new MemorySegment[0]);
            this.memorySegmentCount = this.memorySegments.length;
            this.segmentSize = this.memorySegments[0].size();
            
            clearAllSlots();
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
        
        // 初始化管理数组
        this.extensionBucketBaseIndices = new int[bucketCount];  // 默认0表示未分配
        this.extensionBucketCounts = new int[bucketCount];       // 默认0
        
        // 初始化扩展池状态
        this.extensionPoolCapacity = 0;
        this.extensionPoolUsed = 0;
    }

    /**
     * Gets an entry from the main table using object comparison.
     * 
     * <p>This is the heap object store version that uses {@code store.matches()} 
     * for key/namespace comparison instead of byte comparison.
     *
     * @param keyHash the pre-computed hash value
     * @param tag the tag (high 16 bits of hash)
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the entry address if found, 0 otherwise
     */
    public long get(int keyHash, short tag, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return searchBucketTree(bucketIndex, bucketIndex, tag, key, namespace, store, segment, bucketOffset);
    }

    /**
     * Inserts or updates an entry using object comparison.
     * 
     * <p>This is the heap object store version that uses {@code store.matches()} 
     * for key/namespace comparison instead of byte comparison.
     *
     * @param keyHash the pre-computed hash value
     * @param tag the tag (high 16 bits of hash)
     * @param entryAddress the HeapEntryStore address of the entry
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return 0 for new entry, positive for existing entry address, -1 for full (needs resize)
     */
    public long put(int keyHash, short tag, long entryAddress, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long result = putInBucketTree(bucketIndex, bucketIndex, tag, entryAddress, key, namespace, store);

        if (result == 0) {
            totalEntries++;
            checkResizeNeeded();
        } else if (result == -1) {
            needsResize = true;
        }

        return result;
    }

    /**
     * Removes an entry from the main table using object comparison.
     * 
     * <p>This is the heap object store version that uses {@code store.matches()} 
     * for key/namespace comparison instead of byte comparison.
     *
     * @param keyHash the pre-computed hash value
     * @param tag the tag (high 16 bits of hash)
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore containing the entries
     * @return the removed entry address if found, 0 otherwise
     */
    public long remove(int keyHash, short tag, K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long removed = removeFromBucketTree(bucketIndex, bucketIndex, tag, key, namespace, store, segment, bucketOffset);
        if (removed > 0) {
            totalEntries--;
        }
        return removed;
    }

    public void setSlotPointer(long entryAddress) {
        if (lastFoundSegment != null && lastFountSlotOffset != -1) {
            lastFoundSegment.putLong(lastFountSlotOffset + SLOT_POINTER_OFFSET, entryAddress);
            lastFoundSegment = null;
            lastFountSlotOffset = -1;
        } else {
            throw new IllegalStateException("No slot found to set pointer");
        }
    }

    /**
     * Recursively searches the bucket tree for an entry.
     *
     * @param bucketIndex current bucket's global index
     * @param mainBucketIndex the main bucket index this bucket belongs to
     * @param tag the tag to match
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore
     * @param segment the memory segment containing the bucket
     * @param bucketOffset the offset within the segment
     * @return the entry address if found, 0 otherwise
     */
    private long searchBucketTree(int bucketIndex, int mainBucketIndex, short tag, 
                                  K key, N namespace, HeapEntryStore<K, N, S> store,
                                  MemorySegment segment, int bucketOffset) {
        // Search current bucket's slots first
        long result = searchBucketSlots(segment, bucketOffset, tag, key, namespace, store);
        if (result != 0) return result;

        // Determine extension bucket pointer index based on tag
        int extensionIndex = tag & 0x3;  // Low 2 bits of tag determine which extension pointer

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            return searchBucketTree(extensionBucketIndex, mainBucketIndex, tag, key, namespace, store, 
                                    extensionSegment, extensionBucketOffset);
        }
        return 0;
    }

    /**
     * Searches bucket slots for an entry using object comparison.
     */
    private long searchBucketSlots(MemorySegment segment, int bucketOffset, short tag,
                                   K key, N namespace, HeapEntryStore<K, N, S> store) {
        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            // Use object comparison instead of byte comparison
            if (slotTag == tag && store.matches(ptr, key, namespace)) {
                lastFoundSegment = segment;
                lastFountSlotOffset = slotOffset;
                return ptr;
            }
        }
        return 0;
    }

    /**
     * Recursively inserts into the bucket tree, filling current bucket first, then extension buckets.
     *
     * @param bucketIndex current bucket's global index
     * @param mainBucketIndex the main bucket index this bucket belongs to
     * @param tag the tag
     * @param entryAddress the HeapEntryStore address
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore
     * @return 0 for new entry, positive for updated entry address, -1 for full (needs resize)
     */
    private long putInBucketTree(int bucketIndex, int mainBucketIndex, short tag, long entryAddress, 
                                 K key, N namespace, HeapEntryStore<K, N, S> store) {
        // Try to insert in current bucket first
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long result = putInSlots(segment, bucketOffset, tag, entryAddress, key, namespace, store);
        if (result != -1) return result;

        // Current bucket full, determine extension bucket based on tag
        int extensionIndex = tag & 0x3;  // Low 2 bits of tag determine which extension pointer
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extId == 0) {
            // Allocate new extension bucket
            extId = allocateExtensionBucket(mainBucketIndex);
            if (extId == NULL_BUCKET_ID) return -1;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extId);
        }

        // Recursively insert into extension bucket
        int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, extId);
        return putInBucketTree(extensionBucketIndex, mainBucketIndex, tag, entryAddress, key, namespace, store);
    }

    /**
     * Performs a put operation on bucket slots using object comparison.
     *
     * @return 0 for new entry, positive for existing entry address, -1 for full bucket
     */
    private long putInSlots(MemorySegment segment, int bucketOffset, short tag, long entryAddress,
                            K key, N namespace, HeapEntryStore<K, N, S> store) {
        int empty = -1;
        int emptyOffset = 0;

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (ptr == 0) {
                if (empty == -1) {
                    empty = slot;
                    emptyOffset = slotOffset;
                }
                continue;
            }

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            // Use object comparison instead of byte comparison
            if (slotTag == tag && store.matches(ptr, key, namespace)) {
                lastFoundSegment = segment;
                lastFountSlotOffset = slotOffset;
                if (entryAddress > 0) segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return ptr;
            }
        }

        if (empty != -1) {
            lastFoundSegment = segment;
            lastFountSlotOffset = emptyOffset;
            segment.putShort(emptyOffset + SLOT_TAG_OFFSET, tag);
            if (entryAddress > 0) segment.putLong(emptyOffset + SLOT_POINTER_OFFSET, entryAddress);
            return 0;
        }
        return -1;
    }

    /**
     * Recursively removes an entry from the bucket tree.
     *
     * @param bucketIndex current bucket's global index
     * @param mainBucketIndex the main bucket index this bucket belongs to
     * @param tag the tag
     * @param key the key object
     * @param namespace the namespace object
     * @param store the HeapEntryStore
     * @param segment the memory segment
     * @param bucketOffset the offset
     * @return the removed entry address if found, 0 otherwise
     */
    private long removeFromBucketTree(int bucketIndex, int mainBucketIndex, short tag, 
                                      K key, N namespace, HeapEntryStore<K, N, S> store,
                                      MemorySegment segment, int bucketOffset) {
        // Try to remove from current bucket first
        long removed = removeFromBucketSlots(segment, bucketOffset, tag, key, namespace, store);
        if (removed != 0) return removed;

        // Determine extension bucket pointer index based on tag
        int extensionIndex = tag & 0x3;  // Low 2 bits of tag determine which extension pointer

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            removed = removeFromBucketTree(extensionBucketIndex, mainBucketIndex, tag, key, namespace, store, 
                                           extensionSegment, extensionBucketOffset);
        }
        return removed;
    }

    /**
     * Removes an entry from bucket slots using object comparison.
     */
    private long removeFromBucketSlots(MemorySegment segment, int bucketOffset, short tag, 
                                       K key, N namespace, HeapEntryStore<K, N, S> store) {
        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            // Use object comparison instead of byte comparison
            if (slotTag == tag && store.matches(ptr, key, namespace)) {
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, 0L);
                return ptr;
            }
        }
        return 0;
    }

    // --- Iteration support ---

    /**
     * Iterates over all entries in the main table.
     *
     * @param visitor the visitor to call for each entry
     */
    public void forEachEntry(EntryVisitor visitor) {
        // Iterate over all main buckets
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            visitBucketTree(bucketIndex, bucketIndex, visitor);
        }
    }

    /**
     * Unified bucket tree traversal, recursively visiting bucket and all its extension buckets.
     *
     * @param bucketIndex current bucket's global index
     * @param mainBucketIndex the main bucket index this bucket belongs to
     * @param visitor the visitor to call for each entry
     */
    private void visitBucketTree(int bucketIndex, int mainBucketIndex, EntryVisitor visitor) {
        // Visit all slots in current bucket
        visitBucketSlots(bucketIndex, visitor);

        // Check if this main bucket has extension area
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            return;  // No extension area allocated
        }
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int i = 0; i < EXTENSION_POINTERS; i++) {
            byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
            if (offset != NULL_BUCKET_ID) {
                int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
                visitBucketTree(extensionBucketIndex, mainBucketIndex, visitor);
            }
        }
    }

    private void visitBucketSlots(int bucketIndex, EntryVisitor visitor) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
                visitor.visit(slotPointer, bucketIndex, slotTag);
            }
        }
    }

    // --- Extension bucket management ---

    /**
     * Batch grow the extension bucket memory pool.
     * This is the only entry point for allocating extension area memory,
     * merging multiple small allocations into batch allocations.
     *
     * @param areasToAdd number of extension areas to add
     */
    private void growExtensionPool(int areasToAdd) {
        long totalSize = (long) areasToAdd * EXTENSION_AREA_SIZE;
        try {
            List<MemorySegment> newSegments = allocator.allocate((int) totalSize);
            clearSegmentList(newSegments);
            
            // 扩展数组
            int oldLen = memorySegmentCount;
            int newLen = oldLen + newSegments.size();
            MemorySegment[] newArray = Arrays.copyOf(memorySegments, newLen);
            for (int i = 0; i < newSegments.size(); i++) {
                newArray[oldLen + i] = newSegments.get(i);
            }
            memorySegments = newArray;
            memorySegmentCount = newLen;
            
            allAllocations.add(newSegments);
            extensionPoolCapacity += areasToAdd;
        } catch (Exception e) {
            throw new RuntimeException("Failed to grow extension pool", e);
        }
    }

    /**
     * Allocates an extension bucket for the specified main bucket,
     * returns the offset relative to that main bucket's extension area (1-255).
     *
     * @param mainBucketIndex main bucket index (must be < bucketCount)
     * @return offset (1-255), or NULL_BUCKET_ID(0) for failure
     */
    private byte allocateExtensionBucket(int mainBucketIndex) {
        // Check if single main bucket extension limit is reached
        if (extensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return NULL_BUCKET_ID;
        }
        
        // First extension of this main bucket: allocate an extension area from pool
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            // Batch grow pool when capacity insufficient
            if (extensionPoolUsed >= extensionPoolCapacity) {
                growExtensionPool(POOL_GROW_AREAS);
            }
            // Allocate from pool (O(1) operation, no system call)
            extensionBucketBaseIndices[mainBucketIndex] = bucketCount + extensionPoolUsed * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            extensionPoolUsed++;
        }
        
        // Allocate next extension bucket (offset starts from 1)
        byte offset = (byte) (++extensionBucketCounts[mainBucketIndex]);
        
        // Update statistics
        if (extensionBucketCounts[mainBucketIndex] > maxExtensionBucketsUsed) {
            maxExtensionBucketsUsed = extensionBucketCounts[mainBucketIndex];
        }
        
        return offset;
    }

    /**
     * Converts main bucket index + offset to global bucket index.
     *
     * @param mainBucketIndex main bucket index
     * @param offset extension bucket offset (1-255)
     * @return global bucket index
     */
    private int getExtensionBucketGlobalIndex(int mainBucketIndex, byte offset) {
        int baseIndex = extensionBucketBaseIndices[mainBucketIndex];
        if (baseIndex == 0) {
            throw new IllegalStateException("Extension area not allocated for main bucket " + mainBucketIndex);
        }
        return baseIndex + (offset & 0xFF) - 1;  // offset从1开始，数组索引从0开始
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
                             getMaxExtensionBucketsUsed(), getAllocatedExtensionBuckets(), needsResize);
    }

    // --- Resize operations ---

    /**
     * Tries to resize the table if needed.
     *
     * @param entryStore the HeapEntryStore containing all entries
     */
    public void tryResize(HeapEntryStore<K, N, S> entryStore) {
        if (!needsResize) return;
        resize(entryStore);
    }

    /**
     * Resizes the main table by doubling the bucket count.
     * 
     * <p>This is the heap object store version that uses entry.hash from HeapEntryStore
     * to recalculate bucket positions. Entry addresses remain stable.
     *
     * @param entryStore the HeapEntryStore containing all entries
     */
    public void resize(HeapEntryStore<K, N, S> entryStore) {
        int newBucketCount = bucketCount * 2;
        
        // Only allocate new main bucket memory
        long newTotalSize = (long) newBucketCount * BUCKET_SIZE;

        List<MemorySegment> newMainBucketsAllocation;
        try {
            newMainBucketsAllocation = allocator.allocate((int) newTotalSize);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }

        // New table's management arrays and allocation tracking
        int[] newExtensionBucketBaseIndices = new int[newBucketCount];
        int[] newExtensionBucketCounts = new int[newBucketCount];
        List<List<MemorySegment>> newAllAllocations = new ArrayList<>();
        newAllAllocations.add(newMainBucketsAllocation);  // Track main bucket allocation
        
        // New table's extension pool state
        int[] newPoolState = new int[2];  // [0]=capacity, [1]=used
        
        // Use array storage (will expand dynamically during migration)
        MemorySegment[] newMemorySegmentsArray = newMainBucketsAllocation.toArray(new MemorySegment[0]);
        int newSegmentSize = newMemorySegmentsArray[0].size();
        int[] newSegmentCount = {newMemorySegmentsArray.length};  // Use array to allow modification in inner methods
        MemorySegment[][] newSegmentsHolder = {newMemorySegmentsArray};  // Use holder for expansion
        
        clearSegmentArray(newMemorySegmentsArray, newSegmentCount[0]);

        // Migrate entries directly using HeapEntryStore iteration
        migrateAllEntriesFromHeapStore(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                       newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                       newAllAllocations, newPoolState, entryStore);

        // Release all old table allocations (main buckets and extension buckets)
        for (List<MemorySegment> allocation : allAllocations) {
            allocator.release(allocation);
        }
        
        // Switch to new table
        this.allAllocations.clear();
        this.allAllocations.addAll(newAllAllocations);
        this.memorySegments = newSegmentsHolder[0];
        this.memorySegmentCount = newSegmentCount[0];
        this.segmentSize = newSegmentSize;
        this.extensionBucketBaseIndices = newExtensionBucketBaseIndices;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        this.extensionPoolCapacity = newPoolState[0];
        this.extensionPoolUsed = newPoolState[1];
        needsResize = false;
        maxExtensionBucketsUsed = Arrays.stream(extensionBucketCounts).max().orElse(0);
    }

    private void checkResizeNeeded() {
        if (getLoadFactor() >= loadFactorThreshold || getMaxExtensionBucketsUsed() >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            needsResize = true;
        }
    }

    // --- Resize implementation ---

    /**
     * Migrates all entries from HeapEntryStore to new table.
     * 
     * <p>This is the heap object store version that iterates through HeapEntryStore
     * directly, using cached hash values from entries. Entry addresses remain stable.
     */
    private void migrateAllEntriesFromHeapStore(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount, 
                                                int newSegmentSize, int newBucketCount,
                                                int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                                List<List<MemorySegment>> newAllAllocations, int[] newPoolState, 
                                                HeapEntryStore<K, N, S> entryStore) {
        // Iterate through all active entries in HeapEntryStore
        long maxAddr = entryStore.getMaxAddress();
        for (int index = 0; index < maxAddr; index++) {
            HeapStateEntry<K, N, S> entry = entryStore.getByIndex(index);
            if (entry == null) continue;  // Skip empty slots (deleted entries)
            
            // Use cached hash from entry (no re-computation needed)
            int hash = entry.getHash();
            short tag = entry.getTag();
            long entryAddress = index + 1;  // Address is index + 1
            
            int newBucketIndex = hash & (newBucketCount - 1);
            
            putInNewTable(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                          newBucketIndex, tag, entryAddress, 
                          newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                          newAllAllocations, newPoolState);
        }
    }

    private void putInNewTable(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount, 
                               int newSegmentSize, int newBucketCount, int bucketIndex,
                               short tag, long entryAddress, 
                               int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                               List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        // 确定主桶索引（首次调用时 bucketIndex < newBucketCount 必然成立）
        int mainBucketIndex = bucketIndex < newBucketCount ? bucketIndex : 
                              getMainBucketIndexForNewTable(bucketIndex, newBucketCount, newExtensionBucketBaseIndices);
        putInNewTableWithMainBucket(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                    bucketIndex, mainBucketIndex, tag, entryAddress, 
                                    newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                    newAllAllocations, newPoolState);
    }

    private void putInNewTableWithMainBucket(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount,
                                             int newSegmentSize, int newBucketCount, 
                                             int bucketIndex, int mainBucketIndex,
                                             short tag, long entryAddress, 
                                             int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                             List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        MemorySegment segment = getSegmentForBucket(newSegmentsHolder[0], newSegmentCount[0], newSegmentSize, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, newSegmentSize);

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) {
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return;
            }
        }

        // 当前桶满，根据tag确定扩展桶
        int extensionIndex = tag & 0x3;
        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (offset == NULL_BUCKET_ID) {
            offset = allocateExtensionBucketForNewTable(mainBucketIndex, newSegmentsHolder, newSegmentCount, 
                                                        newSegmentSize, newBucketCount, 
                                                        newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                                        newAllAllocations, newPoolState);
            if (offset == NULL_BUCKET_ID) return;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, offset);
        }

        int extensionBucketIndex = newExtensionBucketBaseIndices[mainBucketIndex] + (offset & 0xFF) - 1;
        putInNewTableWithMainBucket(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                    extensionBucketIndex, mainBucketIndex, tag, entryAddress, 
                                    newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                    newAllAllocations, newPoolState);
    }

    /**
     * Allocates an extension bucket for new table's main bucket (resize only, using pool logic).
     */
    private byte allocateExtensionBucketForNewTable(int mainBucketIndex, 
                                                    MemorySegment[][] newSegmentsHolder, int[] newSegmentCount,
                                                    int newSegmentSize, int newBucketCount,
                                                    int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                                    List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        // Check if single main bucket extension limit is reached
        if (newExtensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return NULL_BUCKET_ID;
        }
        
        // First extension of this main bucket: allocate an extension area from pool
        if (newExtensionBucketBaseIndices[mainBucketIndex] == 0) {
            // Batch grow pool when capacity insufficient
            if (newPoolState[1] >= newPoolState[0]) {
                // Batch allocate extension areas
                long totalSize = (long) POOL_GROW_AREAS * EXTENSION_AREA_SIZE;
                try {
                    List<MemorySegment> newSegments = allocator.allocate((int) totalSize);
                    clearSegmentList(newSegments);
                    
                    // Expand array
                    int oldLen = newSegmentCount[0];
                    int addLen = newSegments.size();
                    MemorySegment[] expanded = Arrays.copyOf(newSegmentsHolder[0], oldLen + addLen);
                    for (int i = 0; i < addLen; i++) {
                        expanded[oldLen + i] = newSegments.get(i);
                    }
                    newSegmentsHolder[0] = expanded;
                    newSegmentCount[0] = oldLen + addLen;
                    
                    newAllAllocations.add(newSegments);
                    newPoolState[0] += POOL_GROW_AREAS;
                } catch (Exception e) {
                    return NULL_BUCKET_ID;
                }
            }
            // Allocate from pool (O(1) operation)
            newExtensionBucketBaseIndices[mainBucketIndex] = newBucketCount + newPoolState[1] * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            newPoolState[1]++;
        }
        
        // Allocate next extension bucket
        byte offset = (byte) (++newExtensionBucketCounts[mainBucketIndex]);
        return offset;
    }

    /**
     * Reverse-calculates main bucket index from global bucket index in new table (resize only).
     */
    private int getMainBucketIndexForNewTable(int globalBucketIndex, int newBucketCount, int[] newExtensionBucketBaseIndices) {
        if (globalBucketIndex < newBucketCount) {
            return globalBucketIndex;
        }
        
        for (int i = 0; i < newBucketCount; i++) {
            int baseIndex = newExtensionBucketBaseIndices[i];
            if (baseIndex > 0 && globalBucketIndex >= baseIndex 
                && globalBucketIndex < baseIndex + MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
                return i;
            }
        }
        
        throw new IllegalStateException("Invalid global bucket index in new table: " + globalBucketIndex);
    }



    // --- Helper methods ---

    /**
     * Gets the MemorySegment containing the specified bucket.
     */
    private MemorySegment getSegmentForBucket(int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segmentSize;
        return memorySegments[Math.min(segmentIndex, memorySegmentCount - 1)];
    }

    /**
     * Resize only: gets segment from new table's segment list.
     */
    private MemorySegment getSegmentForBucket(MemorySegment[] segments, int segmentCount, int segSize, int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segSize;
        return segments[Math.min(segmentIndex, segmentCount - 1)];
    }

    /**
     * Gets bucket offset within segment (using cached segmentSize).
     */
    private int getBucketOffsetInSegment(int bucketIndex) {
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    /**
     * Resize only: calculates offset using specified segmentSize.
     */
    private int getBucketOffsetInSegment(int bucketIndex, int segSize) {
        return ((bucketIndex * BUCKET_SIZE) % segSize);
    }

    private void clearAllSlots() {
        // Use batch operations with reusable buffer for better performance
        clearSegmentArray(memorySegments, memorySegmentCount);
    }

    /**
     * Clears all content in the segment array.
     */
    private void clearSegmentArray(MemorySegment[] segments, int count) {
        byte[] zeroArray = new byte[1024]; // Reusable zero buffer
        for (int i = 0; i < count; i++) {
            MemorySegment segment = segments[i];
            int remaining = segment.size();
            int offset = 0;
            while (remaining > 0) {
                int batchSize = Math.min(remaining, zeroArray.length);
                segment.put(offset, zeroArray, 0, batchSize);
                offset += batchSize;
                remaining -= batchSize;
            }
        }
    }

    /**
     * Clears all content in the segment List (used for resize and similar scenarios).
     */
    private void clearSegmentList(List<MemorySegment> segments) {
        byte[] zeroArray = new byte[1024]; // Reusable zero buffer
        for (MemorySegment segment : segments) {
            int remaining = segment.size();
            int offset = 0;
            while (remaining > 0) {
                int batchSize = Math.min(remaining, zeroArray.length);
                segment.put(offset, zeroArray, 0, batchSize);
                offset += batchSize;
                remaining -= batchSize;
            }
        }
    }

    private int getAllocatedExtensionBuckets() {
        int count = 0;
        for (int i = 0; i < bucketCount; i++) {
            if (extensionBucketBaseIndices[i] > 0) {
                count += extensionBucketCounts[i];  // 累加实际使用的扩展桶数
            }
        }
        return count;
    }

    @FunctionalInterface
    public interface EntryVisitor {
        void visit(long entryAddress, int keyHash, short tag);
    }

    public static class TableStats {
        public final int bucketCount;
        public final int totalEntries;
        public final double loadFactor;
        public final int maxExtensionBuckets;
        public final int allocatedExtensionBuckets;
        public final boolean needsResize;

        public TableStats(int bucketCount, int totalEntries, double loadFactor,
                         int maxExtensionBuckets, int allocatedExtensionBuckets, boolean needsResize) {
            this.bucketCount = bucketCount;
            this.totalEntries = totalEntries;
            this.loadFactor = loadFactor;
            this.maxExtensionBuckets = maxExtensionBuckets;
            this.allocatedExtensionBuckets = allocatedExtensionBuckets;
            this.needsResize = needsResize;
        }

        @Override
        public String toString() {
            return String.format("MainTable[buckets=%d, entries=%d, load=%.2f, maxExt=%d, needsResize=%s]",
                bucketCount, totalEntries, loadFactor, maxExtensionBuckets, needsResize);
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null) {
            // 释放所有跟踪的内存分配（主桶 + 所有动态扩展桶）
            for (List<MemorySegment> allocation : allAllocations) {
                allocator.release(allocation);
            }
            allAllocations.clear();
        }
    }
}
