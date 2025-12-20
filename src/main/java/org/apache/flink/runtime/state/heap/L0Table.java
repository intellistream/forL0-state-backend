package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.L0MemoryAllocator;

import java.util.List;
import java.util.Random;

/**
 * L0 Table implementation for hot key caching in ForL0 State Backend.
 * 
 * <p>This is the heap object store version that uses object comparison instead of
 * byte comparison. The Pointer field now stores the array index in HeapEntryStore
 * instead of off-heap memory address.
 * 
 * <p>Each bucket is 64 bytes aligned and contains 4 slots.
 * 
 * <p>Bucket layout: ValidBitmap(4B) + 4×Slot(15B) = 64B
 * <ul>
 *   <li>ValidBitmap: 4 bytes at bucket start, 1 byte per slot (valid + accessed flags)</li>
 *   <li>Slot layout: Tag(2B) + Extension(5B) + Pointer(8B) = 15B</li>
 * </ul>
 * 
 * <p>Supports configurable replacement algorithms (LRU, LFU, etc.) for cache management.
 *
 * <p>Uses L0MemoryAllocator for memory allocation, which is separate from the
 * MemoryManager-managed memory used by MainTable and HeapEntryStore. L0 memory
 * may be backed by specialized hardware (CXL memory, PMEM) via JNI native methods.
 *
 * <p>Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class L0Table<K, N, S> implements AutoCloseable {

    // L0 bucket and slot layout constants
    private static final int BUCKET_SIZE = 64;  // 64 bytes per bucket
    private static final int BUCKET_SIZE_BITS = 6;  // 64 = 2^6, for fast multiplication via shift
    private static final int SLOTS_PER_BUCKET = 4;  // 4 slots per bucket
    
    // Bucket layout: ValidBitmap (4B) + 4 × Slot (15B each) = 64 bytes
    private static final int VALID_BITMAP_SIZE = 4;
    private static final int SLOT_SIZE = 15;  // 15 bytes per slot

    // Slot field offsets within a slot (relative to slot start)
    private static final int SLOT_TAG_OFFSET = 0;       // 2 bytes
    private static final int SLOT_EXTENSION_OFFSET = 2; // 5 bytes (for LRU/LFU data)
    private static final int SLOT_POINTER_OFFSET = 7;   // 8 bytes

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
    private final MemorySegment[] memorySegments;  // Array for O(1) access
    private final int segmentSizeBits;  // log2(segmentSize) for bit-shift division
    private final int segmentMask;       // segmentSize - 1 for bit-mask modulo
    private final List<MemorySegment> originalAllocation;  // Keep reference for release
    private final ReplacementPolicy replacementPolicy;
    private final Random random;  // For SAMPLED_LRU

    // Statistics and metrics
    // [BENCHMARK_TEST] volatile for thread-safe reads by L0TableMetricsCollector
    private volatile long accessCount = 0;
    private volatile long hitCount = 0;
    private volatile long missCount = 0;
    private volatile long evictionCount = 0;
    
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
            List<MemorySegment> allocation = l0Allocator.allocate((int) totalSize);
            this.originalAllocation = allocation;  // Keep for release
            this.memorySegments = allocation.toArray(new MemorySegment[0]);
            
            // Initialize bit operation fields (segmentSize guaranteed to be power of 2)
            int segmentSize = this.memorySegments[0].size();
            this.segmentSizeBits = Integer.numberOfTrailingZeros(segmentSize);
            this.segmentMask = segmentSize - 1;

            // Initialize all slots as invalid
            clearAllSlots();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate L0 Table memory", e);
        }
    }

    /**
     * Removes an entry from L0 table by entry address.
     */
    public void removeByAddress(long entryAddress) {
        if (entryAddress == 0) return;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);

            int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
                byte valid = segment.get(bucketOffset + slot);
                if (valid == 0) continue;

                long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
                if (pointer == entryAddress) {
                    segment.put(bucketOffset + slot, (byte) 0);
                    return;
                }
            }
        }
    }

    /**
     * Gets an entry from the L0 cache using object comparison.
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
        accessCount++;
        int bucketIndex = keyHash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Check all 4 slots with incremental offset (avoid multiplication)
        int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            byte valid = segment.get(bucketOffset + slot);
            if (valid == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;

            long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            // Use object comparison instead of byte comparison
            if (store.matches(pointer, key, namespace)) {
                hitCount++;
                updateSlotOnAccess(segment, bucketOffset, slot, slotOffset);
                return pointer;
            }
        }

        missCount++;
        return 0;
    }

    /**
     * Puts an entry into the L0 cache using object comparison.
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
     * @return the old entry address if updating, 0 if new entry, or evicted address
     */
    public long put(int keyHash, short tag, long entryAddress, 
                    K key, N namespace, HeapEntryStore<K, N, S> store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Find empty slot or update existing entry
        int emptySlot = -1;
        int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            byte valid = segment.get(bucketOffset + slot);
            if (valid == 0) {
                if (emptySlot == -1) emptySlot = slot;
                continue;
            }

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            // Use object comparison instead of byte comparison
            if (slotTag == tag && store.matches(slotPointer, key, namespace)) {
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                updateSlotOnAccess(segment, bucketOffset, slot, slotOffset);
                return slotPointer;
            }
        }

        // Use empty slot if available
        if (emptySlot != -1) {
            int emptyOffset = bucketOffset + VALID_BITMAP_SIZE + emptySlot * SLOT_SIZE;
            writeSlot(segment, bucketOffset, emptySlot, emptyOffset, tag, entryAddress);
            return 0;
        }

        // No empty slot, need eviction
        int victimSlot = selectVictimSlot(segment, bucketOffset);
        int victimOffset = bucketOffset + VALID_BITMAP_SIZE + victimSlot * SLOT_SIZE;
        long oldEntryAddress = segment.getLong(victimOffset + SLOT_POINTER_OFFSET);

        writeSlot(segment, bucketOffset, victimSlot, victimOffset, tag, entryAddress);
        evictionCount++;

        return oldEntryAddress;
    }

    /**
     * Removes an entry from the L0 cache using object comparison.
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

        int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            byte valid = segment.get(bucketOffset + slot);
            if (valid == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;

            long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            // Use object comparison instead of byte comparison
            if (store.matches(pointer, key, namespace)) {
                segment.put(bucketOffset + slot, (byte) 0);
                return pointer;
            }
        }

        return 0;
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
     * Used when entries in HeapEntryStore are deallocated.
     *
     * @param minAddress Minimum address (inclusive)
     * @param maxAddress Maximum address (exclusive)
     */
    public void invalidateRange(long minAddress, long maxAddress) {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);

            int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
                byte valid = segment.get(bucketOffset + slot);
                if (valid == 0) continue;

                long pointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
                if (pointer >= minAddress && pointer < maxAddress) {
                    segment.put(bucketOffset + slot, (byte) 0);
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
     * Uses bit operations for O(1) access without division.
     */
    private MemorySegment getSegmentForBucket(int bucketIndex) {
        long byteOffset = (long) bucketIndex << BUCKET_SIZE_BITS;
        return memorySegments[(int) (byteOffset >>> segmentSizeBits)];
    }

    /**
     * Gets the offset within the segment for the specified bucket.
     * Uses bit mask for O(1) access without modulo.
     */
    private int getBucketOffsetInSegment(int bucketIndex) {
        return (int) (((long) bucketIndex << BUCKET_SIZE_BITS) & segmentMask);
    }

    private void clearAllSlots() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            segment.putInt(bucketOffset, 0);
        }
    }

    private void writeSlot(MemorySegment segment, int bucketOffset, int slot, int slotOffset, 
                           short tag, long entryAddress) {
        segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
        segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);

        switch (replacementPolicy) {
            case LRU:
                segment.put(bucketOffset + slot, (byte) 1);
                segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, getRelativeTimestamp());
                break;
            case LFU:
            case TINY_LFU:
                segment.put(bucketOffset + slot, (byte) 1);
                segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, 1);
                break;
            case CLOCK:
            case SAMPLED_LRU:
                segment.put(bucketOffset + slot, VALID_MASK);
                break;
            default:
                segment.put(bucketOffset + slot, (byte) 1);
                break;
        }
    }

    private void updateSlotOnAccess(MemorySegment segment, int bucketOffset, int slot, int slotOffset) {
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
                if (++tinyLfuAccessCount >= TINY_LFU_DECAY_INTERVAL) {
                    decayAllFrequencies();
                    tinyLfuAccessCount = 0;
                }
                break;
            case CLOCK:
            case SAMPLED_LRU:
                byte valid = segment.get(bucketOffset + slot);
                segment.put(bucketOffset + slot, (byte)(valid | ACCESSED_MASK));
                break;
            default:
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
            
            int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
                byte valid = segment.get(bucketOffset + slot);
                if ((valid & VALID_MASK) != 0) {
                    int freq = segment.getInt(slotOffset + SLOT_EXTENSION_OFFSET);
                    segment.putInt(slotOffset + SLOT_EXTENSION_OFFSET, freq >> 1);
                }
            }
        }
    }

    private int selectVictimSlot(MemorySegment segment, int bucketOffset) {
        switch (replacementPolicy) {
            case LRU:
            case LFU:
            case TINY_LFU:
                return selectMinExtensionVictim(segment, bucketOffset);
            case CLOCK:
                return selectClockVictim(segment, bucketOffset);
            case SAMPLED_LRU:
                return selectSampledLRUVictim(segment, bucketOffset);
            default:
                return 0;
        }
    }

    /**
     * 通用的victim选择方法：选择extension字段值最小的slot
     */
    private int selectMinExtensionVictim(MemorySegment segment, int bucketOffset) {
        int victimSlot = 0;
        int minValue = Integer.MAX_VALUE;

        int slotOffset = bucketOffset + VALID_BITMAP_SIZE;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            byte valid = segment.get(bucketOffset + slot);
            if (valid == 0) return slot;

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
     */
    private int selectClockVictim(MemorySegment segment, int bucketOffset) {
        // First pass: look for empty or unaccessed slot
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            byte valid = segment.get(bucketOffset + slot);
            if ((valid & VALID_MASK) == 0) return slot;
            if ((valid & ACCESSED_MASK) == 0) return slot;
        }
        
        // Second pass: clear all accessed bits and evict first
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            byte valid = segment.get(bucketOffset + slot);
            segment.put(bucketOffset + slot, (byte)(valid & ~ACCESSED_MASK));
        }
        return 0;
    }

    /**
     * Sampled LRU: randomly sample 2 slots and pick the less recently accessed one.
     */
    private int selectSampledLRUVictim(MemorySegment segment, int bucketOffset) {
        int sample1 = random.nextInt(SLOTS_PER_BUCKET);
        int sample2 = random.nextInt(SLOTS_PER_BUCKET);
        
        byte valid1 = segment.get(bucketOffset + sample1);
        byte valid2 = segment.get(bucketOffset + sample2);
        
        if ((valid1 & VALID_MASK) == 0) return sample1;
        if ((valid2 & VALID_MASK) == 0) return sample2;
        
        boolean accessed1 = (valid1 & ACCESSED_MASK) != 0;
        boolean accessed2 = (valid2 & ACCESSED_MASK) != 0;
        if (!accessed1 && accessed2) return sample1;
        if (accessed1 && !accessed2) return sample2;
        
        return sample1;
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
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                if (segment.get(bucketOffset + slot) != 0) {
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
        l0Allocator.release(originalAllocation);
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
