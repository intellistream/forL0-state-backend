package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;

import java.util.List;
import java.util.Random;

/**
 * L0 Table implementation for hot key caching in ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 4 slots.
 * Each slot contains: tag(2B) + valid(1B) + extension(5B) + pointer(8B) = 16B
 * Supports configurable replacement algorithms (LRU, LFU, etc.) for cache management.
 *
 * <p>Uses L0MemoryAllocator for memory allocation, which is separate from the
 * MemoryManager-managed memory used by MainTable and EntryArena. L0 memory
 * may be backed by specialized hardware (CXL memory, PMEM) via JNI native methods.
 *
 * <p>Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
 */
public class L0Table implements AutoCloseable {

    // L0 bucket and slot layout constants
    private static final int BUCKET_SIZE = 64;  // 64 bytes per bucket
    private static final int SLOTS_PER_BUCKET = 4;  // 4 slots per bucket
    private static final int SLOT_SIZE = 16;  // 16 bytes per slot

    // Slot field offsets within a slot
    private static final int SLOT_TAG_OFFSET = 0;      // 2 bytes
    private static final int SLOT_VALID_OFFSET = 2;    // 1 byte
    private static final int SLOT_EXTENSION_OFFSET = 3; // 5 bytes (for LRU/LFU data)
    private static final int SLOT_POINTER_OFFSET = 8;  // 8 bytes

    // Valid field bit masks for CLOCK algorithm
    private static final byte VALID_MASK = 0x01;       // bit 0: validity
    private static final byte ACCESSED_MASK = 0x02;     // bit 1: accessed flag (for CLOCK)
    
    // LFU constants
    private static final int MAX_FREQUENCY = 15;        // Max frequency for LFU/TinyLFU
    private static final long TINY_LFU_DECAY_INTERVAL = 10000; // Decay every 10K accesses

    // Replacement algorithm types
    public enum ReplacementPolicy {
        LRU,         // Least Recently Used (fixed with relative timestamp)
        LFU,         // Least Frequently Used (with saturation at 15)
        CLOCK,       // Clock algorithm (1-bit accessed flag, recommended default)
        TINY_LFU,    // Window TinyLFU with decay mechanism
        SAMPLED_LRU  // Random sampling + LRU (lightweight)
    }

    private final L0MemoryAllocator l0Allocator;
    private final int bucketCount;
    private final List<MemorySegment> memorySegments;
    private final ReplacementPolicy replacementPolicy;
    private final Random random;  // For SAMPLED_LRU

    // Statistics and metrics
    private long accessCount = 0;
    private long hitCount = 0;
    private long missCount = 0;
    private long evictionCount = 0;
    
    // TinyLFU decay tracking
    private long tinyLfuAccessCount = 0;

    /**
     * Creates an L0 Table with specified number of buckets and CLOCK replacement policy.
     *
     * @param l0Allocator L0 memory allocator for L0 region
     * @param bucketCountPow2 Number of buckets as power of 2
     */
    public L0Table(L0MemoryAllocator l0Allocator, int bucketCountPow2) {
        this(l0Allocator, bucketCountPow2, ReplacementPolicy.CLOCK);
    }

    /**
     * Creates an L0 Table with specified number of buckets and replacement policy.
     *
     * @param l0Allocator L0 memory allocator for L0 region
     * @param bucketCountPow2 Number of buckets as power of 2
     * @param replacementPolicy Replacement algorithm to use
     */
    public L0Table(L0MemoryAllocator l0Allocator, int bucketCountPow2, ReplacementPolicy replacementPolicy) {
        this.l0Allocator = l0Allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.replacementPolicy = replacementPolicy;
        this.random = (replacementPolicy == ReplacementPolicy.SAMPLED_LRU) ? new Random() : null;

        try {
            // Allocate memory segments for L0 table using L0 memory allocator
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            this.memorySegments = l0Allocator.allocate((int) totalSize);

            // Initialize all slots as invalid
            clearAllSlots();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate L0 Table memory", e);
        }
    }

    /**
     * Removes an entry from L0 table by entry address.
     *
     * @param entryAddress Entry address to remove
     */
    public void removeByAddress(long entryAddress) {
        if (entryAddress == 0) return;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // MainTable style: resolve once per bucket
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);

            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                int slotOffset = bucketOffset + slot * SLOT_SIZE;

                byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
                if (valid == 0) continue;

                long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
                if (pointer == entryAddress) {
                    // Mark slot as invalid
                    segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 0);
                    return;
                }
            }
        }
    }

    /** Inline版本：直接传入序列化后key/namespace，避免lambda Matcher开销 */
    public long get(int keyHash, short tag,
                    byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        accessCount++;
        int bucketIndex = keyHash & (bucketCount - 1);
        
        // MainTable style: resolve once per bucket, zero object allocation
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Check all 4 slots in the bucket
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            // Read slot fields directly from segment
            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            if (valid == 0) continue;  // Skip invalid slots

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;  // Tag mismatch

            long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            // Verify actual entry match using arena directly
            if (arena.matchesKey(pointer, kb, klen, nb, nlen)) {
                hitCount++;
                // Update access information for replacement policy
                updateSlotOnAccess(segment, slotOffset);
                return pointer;
            }
        }

        missCount++;
        return 0;  // Not found
    }

    /** Inline版本：直接传入序列化后key/namespace，避免lambda Matcher开销 */
    public long put(int keyHash, short tag, long entryAddress,
                    byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        
        // MainTable style: resolve once per bucket
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // First, try to find an empty slot or update existing entry
        int emptySlot = -1;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            if (valid == 0) {
                // Found empty slot, remember it but continue checking for updates
                if (emptySlot == -1) {
                    emptySlot = slot;
                }
                continue;
            }

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotTag == tag && arena.matchesKey(slotPointer, kb, klen, nb, nlen)) {
                // Found existing entry, update it
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                updateSlotOnAccess(segment, slotOffset);
                return slotPointer;
            }
        }

        // No existing entry found, use empty slot if available
        if (emptySlot != -1) {
            int slotOffset = bucketOffset + emptySlot * SLOT_SIZE;
            writeSlot(segment, slotOffset, tag, entryAddress);
            return 0;  // New insertion
        }

        // No empty slot found, need eviction
        int victimSlot = selectVictimSlot(segment, bucketOffset);
        int victimOffset = bucketOffset + victimSlot * SLOT_SIZE;

        // Get old entry address before overwriting
        long oldEntryAddress = segment.getLong(victimOffset + SLOT_POINTER_OFFSET);

        writeSlot(segment, victimOffset, tag, entryAddress);
        evictionCount++;

        return oldEntryAddress;  // Return evicted entry address
    }

    /** Inline版本：直接传入序列化后key/namespace，避免lambda Matcher开销 */
    public long remove(int keyHash, short tag,
                       byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        
        // MainTable style: resolve once per bucket
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Check all 4 slots in the bucket
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;

            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            if (valid == 0) continue;  // Skip invalid slots

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;  // Tag mismatch

            long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            // Verify actual entry match using arena directly
            if (arena.matchesKey(pointer, kb, klen, nb, nlen)) {
                // Mark slot as invalid
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 0);
                return pointer;
            }
        }

        return 0;  // Not found
    }

    /**
     * Clears all entries in the L0 table.
     */
    public void clear() {
        clearAllSlots();
        accessCount = 0;
        hitCount = 0;
        missCount = 0;
        evictionCount = 0;
        tinyLfuAccessCount = 0;
    }

    /**
     * Invalidates all entries with addresses in the specified range.
     * Used when entries in EntryArena are deallocated.
     *
     * @param minAddress Minimum address (inclusive)
     * @param maxAddress Maximum address (exclusive)
     */
    public void invalidateRange(long minAddress, long maxAddress) {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // MainTable style: resolve once per bucket
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);

            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                int slotOffset = bucketOffset + slot * SLOT_SIZE;

                byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
                if (valid == 0) continue;

                long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
                if (pointer >= minAddress && pointer < maxAddress) {
                    // Mark slot as invalid
                    segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 0);
                }
            }
        }
    }

    /**
     * Gets statistics about the L0 table.
     */
    public L0TableStats getStats() {
        return new L0TableStats(
            getEntryCount(),
            accessCount,
            hitCount,
            missCount,
            evictionCount,
            getLoadFactor()
        );
    }

    // Helper methods for memory access - MainTable style (zero object allocation)

    /**
     * Gets the MemorySegment containing the specified bucket.
     * Following MainTable's implementation pattern.
     */
    private MemorySegment getSegmentForBucket(int bucketIndex) {
        if (memorySegments.isEmpty()) {
            throw new IllegalStateException("No memory segments available");
        }
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / memorySegments.get(0).size();
        return memorySegments.get(Math.min(segmentIndex, memorySegments.size() - 1));
    }

    /**
     * Gets the offset within the segment for the specified bucket.
     * Following MainTable's implementation pattern.
     */
    private int getBucketOffsetInSegment(int bucketIndex) {
        int segmentSize = memorySegments.get(0).size();
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    private void clearAllSlots() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // MainTable style: resolve once per bucket
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                int slotOffset = bucketOffset + slot * SLOT_SIZE;
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 0);
            }
        }
    }

    private void writeSlot(MemorySegment segment, int slotOffset, short tag, long entryAddress) {
        segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
        segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);

        // Initialize based on replacement policy
        switch (replacementPolicy) {
            case LRU:
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 1);
                segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, getRelativeTimestamp());
                break;
            case LFU:
            case TINY_LFU:
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 1);
                segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, 1); // Initial frequency
                break;
            case CLOCK:
            case SAMPLED_LRU:
                // For CLOCK: valid=1, accessed=0 (not yet accessed after insertion)
                // For SAMPLED_LRU: same as CLOCK, use accessed bit
                segment.put(slotOffset + SLOT_VALID_OFFSET, VALID_MASK);
                break;
            default:
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte) 1);
                break;
        }
    }

    private void updateSlotOnAccess(MemorySegment segment, int slotOffset) {
        switch (replacementPolicy) {
            case LRU:
                segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, getRelativeTimestamp());
                break;
            case LFU:
                int freq = segment.getInt(slotOffset + SLOT_EXTENSION_OFFSET);
                if (freq < MAX_FREQUENCY) {
                    segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, freq + 1);
                }
                break;
            case TINY_LFU:
                int tinyFreq = segment.getInt(slotOffset + SLOT_EXTENSION_OFFSET);
                if (tinyFreq < MAX_FREQUENCY) {
                    segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, tinyFreq + 1);
                }
                // Check if we need to decay
                if (++tinyLfuAccessCount >= TINY_LFU_DECAY_INTERVAL) {
                    decayAllFrequencies();
                    tinyLfuAccessCount = 0;
                }
                break;
            case CLOCK:
            case SAMPLED_LRU:
                // Set accessed bit
                byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
                segment.put(slotOffset + SLOT_VALID_OFFSET, (byte)(valid | ACCESSED_MASK));
                break;
            default:
                // No update needed
                break;
        }
    }

    /**
     * Get relative timestamp to avoid overflow issues.
     * Uses lower 31 bits of nanoTime to ensure positive values.
     */
    private int getRelativeTimestamp() {
        return (int)(System.nanoTime() & 0x7FFFFFFF);
    }

    /**
     * Decay all frequencies in the table (for TinyLFU).
     * Divides all frequency counters by 2.
     */
    private void decayAllFrequencies() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                int slotOffset = bucketOffset + slot * SLOT_SIZE;
                byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
                if ((valid & VALID_MASK) != 0) {
                    int freq = segment.getInt(slotOffset + SLOT_EXTENSION_OFFSET);
                    segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, freq >> 1); // Divide by 2
                }
            }
        }
    }

    private int selectVictimSlot(MemorySegment segment, int bucketOffset) {
        switch (replacementPolicy) {
            case LRU:
            case LFU:
            case TINY_LFU:
                // LRU, LFU, TinyLFU都使用extension字段存储时间戳或频率
                // 都是选择最小值的slot，逻辑相同
                return selectMinExtensionVictim(segment, bucketOffset);
            case CLOCK:
                return selectClockVictim(segment, bucketOffset);
            case SAMPLED_LRU:
                return selectSampledLRUVictim(segment, bucketOffset);
            default:
                return 0; // Default to first slot
        }
    }

    /**
     * 通用的victim选择方法：选择extension字段值最小的slot
     * 适用于LRU（时间戳）、LFU（频率）、TinyLFU（频率）
     */
    private int selectMinExtensionVictim(MemorySegment segment, int bucketOffset) {
        int victimSlot = 0;
        int minValue = Integer.MAX_VALUE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            
            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            if (valid == 0) return slot; // Use empty slot first

            int value = segment.getInt(slotOffset + SLOT_EXTENSION_OFFSET);
            if (value < minValue) {
                minValue = value;
                victimSlot = slot;
            }
        }
        return victimSlot;
    }

    /**
     * CLOCK algorithm: uses a single bit to approximate LRU.
     * Two-pass algorithm:
     * 1. First pass: find a slot with accessed=0
     * 2. Second pass: clear all accessed bits and return slot 0
     */
    private int selectClockVictim(MemorySegment segment, int bucketOffset) {
        // First pass: look for unaccessed slot
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            
            if ((valid & VALID_MASK) == 0) return slot; // Empty slot
            
            if ((valid & ACCESSED_MASK) == 0) {
                // Found unaccessed slot, evict it
                return slot;
            }
        }
        
        // Second pass: all slots accessed, clear accessed bits and evict first
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
            segment.put(slotOffset + SLOT_VALID_OFFSET, (byte)(valid & ~ACCESSED_MASK));
        }
        return 0;
    }

    /**
     * Sampled LRU: randomly sample 2 slots and pick the less recently accessed one.
     * Provides ~80% of LRU performance with minimal overhead.
     */
    private int selectSampledLRUVictim(MemorySegment segment, int bucketOffset) {
        // Sample two random slots
        int sample1 = random.nextInt(SLOTS_PER_BUCKET);
        int sample2 = random.nextInt(SLOTS_PER_BUCKET);
        
        int offset1 = bucketOffset + sample1 * SLOT_SIZE;
        int offset2 = bucketOffset + sample2 * SLOT_SIZE;
        
        byte valid1 = segment.get(offset1 + SLOT_VALID_OFFSET);
        byte valid2 = segment.get(offset2 + SLOT_VALID_OFFSET);
        
        // Prefer empty slots
        if ((valid1 & VALID_MASK) == 0) return sample1;
        if ((valid2 & VALID_MASK) == 0) return sample2;
        
        // Compare accessed bits
        boolean accessed1 = (valid1 & ACCESSED_MASK) != 0;
        boolean accessed2 = (valid2 & ACCESSED_MASK) != 0;
        
        // Pick the unaccessed one, or the first if both are same
        if (!accessed1 && accessed2) return sample1;
        if (accessed1 && !accessed2) return sample2;
        
        return sample1; // Both same, pick first
    }

    // Statistics and metrics methods

    public long getAccessCount() {
        return accessCount;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public double getHitRate() {
        return accessCount == 0 ? 0.0 : (double) hitCount / accessCount;
    }

    public double getMissRate() {
        return accessCount == 0 ? 0.0 : (double) missCount / accessCount;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public ReplacementPolicy getReplacementPolicy() {
        return replacementPolicy;
    }

    /**
     * Gets the total number of valid entries in the L0 table.
     */
    public int getEntryCount() {
        int count = 0;
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // MainTable style: resolve once per bucket
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                int slotOffset = bucketOffset + slot * SLOT_SIZE;
                byte valid = segment.get(slotOffset + SLOT_VALID_OFFSET);
                if (valid != 0) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Gets the load factor of the L0 table.
     */
    public double getLoadFactor() {
        return (double) getEntryCount() / (bucketCount * SLOTS_PER_BUCKET);
    }

    @Override
    public void close() {
        if (memorySegments != null) {
            l0Allocator.release(memorySegments);
        }
    }

    /**
     * Statistics class for L0 table metrics.
     */
    public static class L0TableStats {
        public final int validSlots;
        public final int totalSlots;
        public final long accessCount;
        public final long hitCount;
        public final long missCount;
        public final long evictionCount;
        public final double loadFactor;
        public final double hitRate;
        public final double missRate;

        public L0TableStats(int validSlots, long accessCount, long hitCount,
                          long missCount, long evictionCount, double loadFactor) {
            this.validSlots = validSlots;
            this.totalSlots = 16; // 4 buckets × 4 slots per bucket
            this.accessCount = accessCount;
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.loadFactor = loadFactor;
            this.hitRate = accessCount == 0 ? 0.0 : (double) hitCount / accessCount;
            this.missRate = accessCount == 0 ? 0.0 : (double) missCount / accessCount;
        }

        @Override
        public String toString() {
            return String.format(
                "L0TableStats{validSlots=%d, totalSlots=%d, accessCount=%d, " +
                "hitCount=%d, missCount=%d, evictionCount=%d, loadFactor=%.3f, " +
                "hitRate=%.3f, missRate=%.3f, buckets=4, policy=LRU}",
                validSlots, totalSlots, accessCount, hitCount, missCount,
                evictionCount, loadFactor, hitRate, missRate
            );
        }
    }

}
