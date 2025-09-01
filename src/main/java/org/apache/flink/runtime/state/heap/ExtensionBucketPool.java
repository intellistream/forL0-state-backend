package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.List;

/**
 * Extension Bucket Pool for managing extension buckets in Main Table.
 * Supports up to 255 extension buckets per main bucket.
 * Single-threaded implementation as Flink Task state access is single-threaded.
 *
 * Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
 */
public class ExtensionBucketPool implements AutoCloseable {

    private static final int BUCKET_SIZE = 64;  // Same as main bucket size
    private static final int SLOTS_PER_BUCKET = 6;  // 6 slots per bucket
    private static final int SLOT_SIZE = 10;  // tag(2B) + pointer(8B) = 10B
    private static final byte NULL_BUCKET_ID = 0;

    // Slot field offsets within a slot (same as MainTable)
    private static final int SLOT_TAG_OFFSET = 0;      // 2 bytes
    private static final int SLOT_POINTER_OFFSET = 2;  // 8 bytes

    private final MemoryManagerAllocator allocator;
    private final int maxBuckets;
    private final List<MemorySegment> memorySegments;
    private final long totalSize;

    // Free list for bucket allocation (single-threaded)
    // 将 nextFreeBucketId 改为int，避免超过127时byte溢出为负
    private int nextFreeBucketId; // 下一个候选可用桶ID (1..maxBuckets+1)
    private final boolean[] bucketInUse;

    /**
     * Creates an extension bucket pool.
     *
     * @param allocator Memory allocator
     * @param maxBuckets Maximum number of extension buckets (up to 255)
     */
    public ExtensionBucketPool(MemoryManagerAllocator allocator, int maxBuckets) {
        if (maxBuckets <= 0 || maxBuckets > 255) {
            throw new IllegalArgumentException("Maximum buckets must be between 1 and 255, but was: " + maxBuckets);
        }

        this.allocator = allocator;
        this.maxBuckets = maxBuckets;
        this.totalSize = (long) maxBuckets * BUCKET_SIZE;
        this.bucketInUse = new boolean[maxBuckets + 1]; // +1 because bucket ID 0 is reserved as NULL
        this.nextFreeBucketId = 1; // Start from 1, as 0 is reserved for NULL

        try {
            // Allocate memory segments for extension buckets
            int numPages = (int) ((totalSize + allocator.getPageSize() - 1) / allocator.getPageSize());
            this.memorySegments = allocator.allocate(numPages);

            // Clear all buckets
            clearAllBuckets();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Extension Bucket Pool memory", e);
        }
    }

    /**
     * Allocates a new extension bucket.
     *
     * @return Bucket ID (1-255), or 0 if allocation failed
     */
    public byte allocateBucket() {
        // 优先从 nextFreeBucketId 往后找
        for (int id = nextFreeBucketId; id <= maxBuckets; id++) {
            if (!bucketInUse[id]) {
                bucketInUse[id] = true;

                // Clear the allocated bucket
                clearBucketInternal(id);

                // Update next free bucket hint
                updateNextFreeBucketId(id + 1);

                return (byte) id; // 仍用 byte 返回，写入主表
            }
        }
        // 回绕从1开始找直到 nextFreeBucketId-1
        for (int id = 1; id < nextFreeBucketId; id++) {
            if (!bucketInUse[id]) {
                bucketInUse[id] = true;

                // Clear the allocated bucket
                clearBucketInternal(id);

                // Update next free bucket hint
                updateNextFreeBucketId(id + 1);

                return (byte) id;
            }
        }

        return NULL_BUCKET_ID; // 池耗尽
    }

    /**
     * Frees an extension bucket.
     *
     * @param bucketId Bucket ID to free
     */
    public void freeBucket(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || id > maxBuckets) {
            return; // Invalid bucket ID
        }

        if (bucketInUse[id]) {
            bucketInUse[id] = false;

            // Clear the freed bucket
            clearBucketInternal(id);

            // Update free bucket hint
            if (id < nextFreeBucketId) {
                nextFreeBucketId = id; // 回缩指针
            }
        }
    }

    /**
     * Gets the memory address of an extension bucket.
     * Note: This is kept for compatibility, but new methods should use MemorySegment directly.
     *
     * @param bucketId Bucket ID (1-255)
     * @return Memory address of the bucket
     */
    public long getBucketAddress(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || id > maxBuckets) {
            throw new IllegalArgumentException("Invalid bucket ID: " + id);
        }

        // For compatibility with MainTable's current interface
        // This method should be deprecated in favor of direct MemorySegment access
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        return segment.getAddress() + bucketOffset;
    }

    /**
     * Searches for an entry in the specified bucket.
     *
     * @param bucketAddress Bucket address (from getBucketAddress)
     * @param tag Tag to search for
     * @param entryMatcher Matcher to verify entry
     * @return Entry address if found, 0 if not found
     */
    public long searchInBucket(long bucketAddress, short tag, MainTable.EntryMatcher entryMatcher) {
        byte bucketId = findBucketIdByAddress(bucketAddress);
        return searchInBucket(bucketId, tag, entryMatcher);
    }

    /**
     * Searches for an entry in the specified bucket.
     *
     * @param bucketId Bucket ID
     * @param tag Tag to search for
     * @param entryMatcher Matcher to verify entry
     * @return Entry address if found, 0 if not found
     */
    public long searchInBucket(byte bucketId, short tag, MainTable.EntryMatcher entryMatcher) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) {
            return 0;
        }

        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) continue;  // Empty slot
            if (slotTag != tag) continue;    // Tag mismatch

            if (entryMatcher.matches(slotPointer)) {
                return slotPointer;
            }
        }
        return 0;
    }

    // --- inline 版本：直接传 key/namespace，绕过 EntryMatcher ---
    public long searchInBucketInline(byte bucketId, short tag,
                                     byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) return 0;
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;
            if (arena.matchesKey(ptr, kb, klen, nb, nlen)) {
                return ptr;
            }
        }
        return 0;
    }

    public long putInBucketInline(byte bucketId, short tag, long entryAddress,
                                  byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) return -1;
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        int empty = -1;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) { if (empty == -1) empty = slot; continue; }
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag == tag && arena.matchesKey(ptr, kb, klen, nb, nlen)) {
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return ptr; // update
            }
        }
        if (empty != -1) {
            int slotOffset = bucketOffset + empty * SLOT_SIZE;
            segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
            segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
            return 0; // new
        }
        return -1; // full
    }

    public long removeFromBucketInline(byte bucketId, short tag,
                                       byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) return 0;
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;
            if (arena.matchesKey(ptr, kb, klen, nb, nlen)) {
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, (short)0);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, 0L);
                return ptr;
            }
        }
        return 0;
    }

    /**
     * Puts an entry into the specified bucket.
     *
     * @param bucketAddress Bucket address (from getBucketAddress)
     * @param tag Tag for the entry
     * @param entryAddress Entry address in EntryArena
     * @param entryMatcher Matcher to verify existing entries
     * @return Previous entry address if updated, 0 if newly inserted, -1 if bucket is full
     */
    public long putInBucket(long bucketAddress, short tag, long entryAddress, MainTable.EntryMatcher entryMatcher) {
        byte bucketId = findBucketIdByAddress(bucketAddress);
        return putInBucket(bucketId, tag, entryAddress, entryMatcher);
    }

    /**
     * Puts an entry into the specified bucket.
     *
     * @param bucketId Bucket ID
     * @param tag Tag for the entry
     * @param entryAddress Entry address in EntryArena
     * @param entryMatcher Matcher to verify existing entries
     * @return Previous entry address if updated, 0 if newly inserted, -1 if bucket is full
     */
    public long putInBucket(byte bucketId, short tag, long entryAddress, MainTable.EntryMatcher entryMatcher) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) {
            return -1;
        }

        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        int emptySlot = -1;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                if (emptySlot == -1) emptySlot = slot;
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

    /**
     * Removes an entry from the specified bucket.
     *
     * @param bucketAddress Bucket address (from getBucketAddress)
     * @param tag Tag to search for
     * @param entryMatcher Matcher to verify entry
     * @return Removed entry address if found, 0 if not found
     */
    public long removeFromBucket(long bucketAddress, short tag, MainTable.EntryMatcher entryMatcher) {
        byte bucketId = findBucketIdByAddress(bucketAddress);
        return removeFromBucket(bucketId, tag, entryMatcher);
    }

    /**
     * Removes an entry from the specified bucket.
     *
     * @param bucketId Bucket ID
     * @param tag Tag to search for
     * @param entryMatcher Matcher to verify entry
     * @return Removed entry address if found, 0 if not found
     */
    public long removeFromBucket(byte bucketId, short tag, MainTable.EntryMatcher entryMatcher) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) {
            return 0;
        }

        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) continue;
            if (slotTag != tag) continue;

            if (entryMatcher.matches(slotPointer)) {
                // Clear the slot
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, (short)0);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, 0L);
                return slotPointer;
            }
        }
        return 0;
    }

    /**
     * Checks if a bucket is empty.
     *
     * @param bucketAddress Bucket address (from getBucketAddress)
     * @return true if bucket is empty, false otherwise
     */
    public boolean isBucketEmpty(long bucketAddress) {
        byte bucketId = findBucketIdByAddress(bucketAddress);
        return isBucketEmpty(bucketId);
    }

    /**
     * Checks if a bucket is empty.
     *
     * @param bucketId Bucket ID
     * @return true if bucket is empty, false otherwise
     */
    public boolean isBucketEmpty(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) {
            return true;
        }

        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) return false;
        }
        return true;
    }

    /**
     * Visits all slots in the specified bucket.
     *
     * @param bucketAddress Bucket address (from getBucketAddress)
     * @param visitor Visitor to call for each entry
     * @param bucketIndex Main bucket index (for visitor context)
     */
    public void visitBucketSlots(long bucketAddress, MainTable.EntryVisitor visitor, int bucketIndex) {
        byte bucketId = findBucketIdByAddress(bucketAddress);
        visitBucketSlots(bucketId, visitor, bucketIndex);
    }

    /**
     * Visits all slots in the specified bucket.
     *
     * @param bucketId Bucket ID
     * @param visitor Visitor to call for each entry
     * @param bucketIndex Main bucket index (for visitor context)
     */
    public void visitBucketSlots(byte bucketId, MainTable.EntryVisitor visitor, int bucketIndex) {
        int id = bucketId & 0xFF;
        if (id == 0 || !bucketInUse[id]) return;
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                visitor.visit(slotPointer, bucketIndex, slotTag);
            }
        }
    }

    /**
     * Checks if a bucket is currently in use.
     *
     * @param bucketId Bucket ID to check
     * @return true if bucket is in use, false otherwise
     */
    public boolean isBucketInUse(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || id > maxBuckets) return false;
        return bucketInUse[id];
    }

    /**
     * Gets the number of allocated buckets.
     */
    public int getAllocatedBuckets() {
        int count = 0;
        for (int i = 1; i <= maxBuckets; i++) {
            if (bucketInUse[i]) count++;
        }
        return count;
    }

    /**
     * Gets statistics about pool usage.
     */
    public PoolStats getStats() {
        int bucketsInUse = getAllocatedBuckets();
        int bucketsAvailable = maxBuckets - bucketsInUse;
        double utilizationRate = maxBuckets > 0 ? (double) bucketsInUse / maxBuckets : 0.0;

        return new PoolStats(maxBuckets, bucketsInUse, bucketsAvailable, utilizationRate);
    }

    private void updateNextFreeBucketId(int start) {
        // Find next free bucket starting from current hint
        for (int id = Math.max(1, start); id <= maxBuckets; id++) {
            if (!bucketInUse[id]) {
                nextFreeBucketId = id;
                return;
            }
        }
        // 没有空闲，指向 maxBuckets+1
        nextFreeBucketId = maxBuckets + 1;
    }

    // 保留原 clearBucket(byte) API，改为内部使用int实现
    private void clearBucket(byte bucketId) { clearBucketInternal(bucketId & 0xFF); }
    private void clearBucketInternal(int id) {
        if (id <= 0 || id > maxBuckets) return;
        MemorySegment segment = getSegmentForBucketInternal(id);
        int bucketOffset = getBucketOffsetInSegmentInternal(id);
        // Clear the entire bucket (64 bytes)
        for (int offset = 0; offset < BUCKET_SIZE; offset += 8) {
            segment.putLong(bucketOffset + offset, 0L);
        }
    }

    private MemorySegment getSegmentForBucket(byte bucketId) { return getSegmentForBucketInternal(bucketId & 0xFF); }
    private MemorySegment getSegmentForBucketInternal(int id) {
        int bucketIndex = id - 1;
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / memorySegments.get(0).size();
        return memorySegments.get(Math.min(segmentIndex, memorySegments.size() - 1));
    }
    private int getBucketOffsetInSegment(byte bucketId) { return getBucketOffsetInSegmentInternal(bucketId & 0xFF); }
    private int getBucketOffsetInSegmentInternal(int id) {
        int bucketIndex = id - 1;
        int segmentSize = memorySegments.get(0).size();
        return (bucketIndex * BUCKET_SIZE) % segmentSize;
    }

    private byte findBucketIdByAddress(long bucketAddress) {
        for (int id = 1; id <= maxBuckets; id++) {
            if (bucketInUse[id]) {
                MemorySegment segment = getSegmentForBucketInternal(id);
                int bucketOffset = getBucketOffsetInSegmentInternal(id);
                long calculatedAddress = segment.getAddress() + bucketOffset;
                if (calculatedAddress == bucketAddress) {
                    return (byte) id;
                }
            }
        }
        throw new IllegalArgumentException("Invalid bucket address: " + bucketAddress);
    }

    @Override
    public void close() throws Exception {
        if (allocator != null && memorySegments != null) {
            allocator.release(memorySegments);
        }
    }

    /**
     * Statistics about the extension bucket pool.
     */
    public static class PoolStats {
        public final int maxBuckets;
        public final int bucketsInUse;
        public final int bucketsAvailable;
        public final double utilizationRate;

        public PoolStats(int maxBuckets, int bucketsInUse, int bucketsAvailable, double utilizationRate) {
            this.maxBuckets = maxBuckets;
            this.bucketsInUse = bucketsInUse;
            this.bucketsAvailable = bucketsAvailable;
            this.utilizationRate = utilizationRate;
        }

        @Override
        public String toString() {
            return String.format("PoolStats[max=%d, inUse=%d, available=%d, utilization=%.2f%%]",
                    maxBuckets, bucketsInUse, bucketsAvailable, utilizationRate * 100);
        }
    }

    // 重新加入clearAllBuckets实现（之前被覆盖删除）
    private void clearAllBuckets() {
        if (memorySegments == null) return;
        for (MemorySegment segment : memorySegments) {
            for (int offset = 0; offset < segment.size(); offset += 8) {
                segment.putLong(offset, 0L);
            }
        }
    }
}
