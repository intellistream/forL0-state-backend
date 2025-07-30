package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.List;

/**
 * Main Table implementation for ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 6 slots + 4 extension pointers.
 * Supports local tree-like expansion for collision resolution.
 * Includes load factor monitoring and global resize triggering.
 * 
 * Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
 */
public class MainTable implements AutoCloseable {

    // Main table layout constants
    private static final int BUCKET_SIZE = 64;  // 64 bytes per bucket
    private static final int SLOTS_PER_BUCKET = 6;  // 6 slots per bucket
    private static final int SLOT_SIZE = 10;  // tag(2B) + pointer(8B) = 10B
    private static final int EXTENSION_POINTERS = 4;  // 4 extension pointers, 1B each

    // Slot field offsets within a slot
    private static final int SLOT_TAG_OFFSET = 0;      // 2 bytes
    private static final int SLOT_POINTER_OFFSET = 2;  // 8 bytes

    // Extension pointers offset (after 6 slots = 60 bytes)
    private static final int EXTENSION_POINTERS_OFFSET = 60;  // 4 bytes for extension pointers

    // Resize thresholds
    private static final double DEFAULT_LOAD_FACTOR_THRESHOLD = 0.75;
    private static final int MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET = 255;

    private final MemoryManagerAllocator allocator;
    private final int bucketCount;
    private final List<MemorySegment> memorySegments;

    // Extension bucket pool
    private final ExtensionBucketPool extensionPool;

    // Load monitoring
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    
    // Extension bucket tracking - avoid recalculating every time
    private int maxExtensionBucketsUsed = 0;
    private final int[] extensionBucketCounts; // Track extension buckets per main bucket

    /**
     * Creates a Main Table with specified number of buckets.
     *
     * @param allocator Memory allocator
     * @param bucketCountPow2 Number of buckets as power of 2
     */
    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this(allocator, bucketCountPow2, DEFAULT_LOAD_FACTOR_THRESHOLD);
    }

    /**
     * Creates a Main Table with specified number of buckets and load factor threshold.
     *
     * @param allocator Memory allocator
     * @param bucketCountPow2 Number of buckets as power of 2
     * @param loadFactorThreshold Threshold for triggering global resize
     */
    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2, double loadFactorThreshold) {
        this.allocator = allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.loadFactorThreshold = loadFactorThreshold;

        try {
            // Allocate memory segments for main table
            int numPages = (int) ((bucketCount * BUCKET_SIZE + allocator.getPageSize() - 1) / allocator.getPageSize());
            this.memorySegments = allocator.allocate(numPages);

            // Initialize extension bucket pool
            this.extensionPool = new ExtensionBucketPool(allocator, bucketCount * 4); // Initial pool size

            // Clear all buckets
            clearAllBuckets();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }

        // Initialize extension bucket counts
        this.extensionBucketCounts = new int[bucketCount];
    }

    /**
     * Gets a value from main table by key hash and tag.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryMatcher Function to verify actual entry match using EntryArena
     * @return Entry address if found, 0 if not found
     */
    public long get(int keyHash, short tag, EntryMatcher entryMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);

        // First check main bucket slots
        long result = searchBucketSlots(bucketIndex, tag, entryMatcher);
        if (result != 0) {
            return result;
        }

        // Check extension buckets if main bucket is full
        return searchExtensionBuckets(bucketIndex, tag, entryMatcher);
    }

    /**
     * Puts a key-value entry into main table with automatic resizing support.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryAddress Address of entry in EntryArena
     * @param entryMatcher Function to verify entry match for updates
     * @return Previous entry address if updated, 0 if newly inserted
     */
    public long put(int keyHash, short tag, long entryAddress, EntryMatcher entryMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);

        // First try to update existing entry or find empty slot in main bucket
        long oldPointer = putInBucketSlots(bucketIndex, tag, entryAddress, entryMatcher);
        if (oldPointer != -1) {
            if (oldPointer == 0) {
                // New insertion
                totalEntries++;
                checkResizeNeeded();
            }
            return oldPointer;  // Successfully handled in main bucket
        }

        // Main bucket is full, need extension bucket
        long result = putInExtensionBuckets(bucketIndex, tag, entryAddress, entryMatcher);
        if (result != -1) {
            if (result == 0) {
                // New insertion
                totalEntries++;
                checkResizeNeeded();
            }
            return result;
        }

        // Extension buckets are also full, trigger resize flag and throw exception
        needsResize = true;
        throw new RuntimeException("Table is full - resize needed");
    }

    /**
     * Removes an entry from main table.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryMatcher Function to verify actual entry match
     * @return Address of removed entry if found, 0 if not found
     */
    public long remove(int keyHash, short tag, EntryMatcher entryMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);

        // First check main bucket slots
        long removedPointer = removeFromBucketSlots(bucketIndex, tag, entryMatcher);
        if (removedPointer != 0) {
            totalEntries--;
            return removedPointer;
        }

        // Check extension buckets
        long removedFromExtension = removeFromExtensionBuckets(bucketIndex, tag, entryMatcher);
        if (removedFromExtension != 0) {
            totalEntries--;
        }
        return removedFromExtension;
    }

    /**
     * Gets the current load factor of the main table.
     */
    public double getLoadFactor() {
        int totalSlots = bucketCount * SLOTS_PER_BUCKET;
        return totalSlots > 0 ? (double) totalEntries / totalSlots : 0.0;
    }

    /**
     * Checks if the table needs to be resized.
     */
    public boolean needsResize() {
        return needsResize;
    }

    /**
     * Gets the maximum number of extension buckets for any main bucket.
     */
    public int getMaxExtensionBucketsUsed() {
        return maxExtensionBucketsUsed;
    }

    /**
     * Gets statistics about the table.
     */
    public TableStats getStats() {
        return new TableStats(
            bucketCount,
            totalEntries,
            getLoadFactor(),
            getMaxExtensionBucketsUsed(),
            extensionPool.getAllocatedBuckets(),
            needsResize
        );
    }

    private void checkResizeNeeded() {
        if (getLoadFactor() >= loadFactorThreshold || getMaxExtensionBucketsUsed() >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            needsResize = true;
        }
    }

    private void updateExtensionBucketCount(int bucketIndex, int delta) {
        extensionBucketCounts[bucketIndex] += delta;
        if (extensionBucketCounts[bucketIndex] > maxExtensionBucketsUsed) {
            maxExtensionBucketsUsed = extensionBucketCounts[bucketIndex];
        }
    }

    private long searchBucketSlots(int bucketIndex, short tag, EntryMatcher entryMatcher) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                continue;  // Empty slot
            }
            if (slotTag != tag) {
                continue;    // Tag mismatch
            }

            if (entryMatcher.matches(slotPointer)) {
                return slotPointer;
            }
        }
        return 0;
    }

    private long searchExtensionBuckets(int bucketIndex, short tag, EntryMatcher entryMatcher) {
        int extensionIndex = tag & 0x3;  // Use tag's low 2 bits to select extension
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        
        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            return 0;  // No extension bucket
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        return searchBucketSlotsInExtension(extensionBucketAddress, tag, entryMatcher);
    }

    private long searchBucketSlotsInExtension(long bucketAddress, short tag, EntryMatcher entryMatcher) {
        // This method works with extension buckets using absolute addresses from ExtensionBucketPool
        // For now, we'll need to coordinate with ExtensionBucketPool's memory layout
        // This is a simplified implementation that assumes ExtensionBucketPool provides MemorySegment access
        return extensionPool.searchInBucket(bucketAddress, tag, entryMatcher);
    }

    private long putInBucketSlots(int bucketIndex, short tag, long entryAddress, EntryMatcher entryMatcher) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        int emptySlot = -1;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                if (emptySlot == -1) {
                    emptySlot = slot;
                }
                continue;
            }

            if (slotTag == tag && entryMatcher.matches(slotPointer)) {
                // Update existing entry
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return slotPointer;
            }
        }

        // Insert in empty slot if available
        if (emptySlot != -1) {
            int slotOffset = bucketOffset + emptySlot * SLOT_SIZE;
            segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
            segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
            return 0;  // New insertion
        }

        return -1;  // Bucket is full
    }

    private long putInExtensionBuckets(int bucketIndex, short tag, long entryAddress, EntryMatcher entryMatcher) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        
        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            // Allocate new extension bucket
            extensionBucketId = extensionPool.allocateBucket();
            if (extensionBucketId == 0) {
                return -1;  // Pool exhausted
            }
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extensionBucketId);
            updateExtensionBucketCount(bucketIndex, 1);
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        return extensionPool.putInBucket(extensionBucketAddress, tag, entryAddress, entryMatcher);
    }

    private long removeFromBucketSlots(int bucketIndex, short tag, EntryMatcher entryMatcher) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                continue;
            }
            if (slotTag != tag) {
                continue;
            }

            if (entryMatcher.matches(slotPointer)) {
                // Clear the slot
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, (short) 0);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, 0L);
                return slotPointer;
            }
        }
        return 0;
    }

    private long removeFromExtensionBuckets(int bucketIndex, short tag, EntryMatcher entryMatcher) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        
        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            return 0;  // No extension bucket
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        long removedPointer = extensionPool.removeFromBucket(extensionBucketAddress, tag, entryMatcher);

        // Check if extension bucket is now empty and can be freed
        if (removedPointer != 0 && extensionPool.isBucketEmpty(extensionBucketAddress)) {
            extensionPool.freeBucket(extensionBucketId);
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, (byte) 0);
            updateExtensionBucketCount(bucketIndex, -1);
        }

        return removedPointer;
    }

    private void visitBucketSlots(int bucketIndex, EntryVisitor visitor) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer != 0) {
                visitor.visit(slotPointer, bucketIndex, slotTag);
            }
        }
    }

    private void visitExtensionBuckets(int bucketIndex, EntryVisitor visitor) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int i = 0; i < EXTENSION_POINTERS; i++) {
            byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
            if (extensionBucketId != 0) {
                long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
                extensionPool.visitBucketSlots(extensionBucketAddress, visitor, bucketIndex);
            }
        }
    }

    private void clearAllBuckets() {
        // Zero out all memory segments
        for (MemorySegment segment : memorySegments) {
            for (int offset = 0; offset < segment.size(); offset += 8) {
                segment.putLong(offset, 0L);
            }
        }
    }

    private MemorySegment getSegmentForBucket(int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / memorySegments.get(0).size();
        return memorySegments.get(Math.min(segmentIndex, memorySegments.size() - 1));
    }

    private int getBucketOffsetInSegment(int bucketIndex) {
        int segmentSize = memorySegments.get(0).size();
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    @Override
    public void close() throws Exception {
        if (extensionPool != null) {
            extensionPool.close();
        }
        if (allocator != null && memorySegments != null) {
            allocator.release(memorySegments);
        }
    }

    /**
     * Interface for matching entries using EntryArena.
     */
    @FunctionalInterface
    public interface EntryMatcher {
        boolean matches(long entryAddress);
    }

    /**
     * Interface for visiting entries during iteration.
     */
    @FunctionalInterface
    public interface EntryVisitor {
        void visit(long entryAddress, int keyHash, short tag);
    }

    /**
     * Statistics about the main table.
     */
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
}
