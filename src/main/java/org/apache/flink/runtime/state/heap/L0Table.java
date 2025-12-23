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
 * <p>Each bucket is 64 bytes aligned and contains 7 slots + 1 extension long.
 * 
 * <p>Bucket layout (64 bytes = 8 longs):
 * <ul>
 *   <li>Slots 0-6: 7 × 8 bytes = 56 bytes, each slot = [Hash(32b) | Ptr(32b)]</li>
 *   <li>Extension: 8 bytes at offset 56, used for CLOCK accessed bits</li>
 * </ul>
 * 
 * <p>Slot format (8 bytes = 1 long):
 * <pre>
 * 63                              32 31                        0
 * ┌────────────────────────────────┬────────────────────────────┐
 * │         Hash (32 bits)         │       Pointer (32 bits)    │
 * └────────────────────────────────┴────────────────────────────┘
 * </pre>
 * 
 * <p>Empty slot: slot == 0 (entire long is zero)
 * 
 * <p>Supports multiple replacement policies (CLOCK, LRU, LFU, TINY_LFU, SAMPLED_LRU).
 * The extension long at offset 56 stores replacement algorithm metadata:
 * <ul>
 *   <li>CLOCK/SAMPLED_LRU: bits 0-6 = 7 accessed flags (1 bit per slot)</li>
 *   <li>LFU/TINY_LFU: 7 × 8 bits = frequency counters (0-255 per slot)</li>
 *   <li>LRU: 7 × 9 bits = relative timestamps (0-511 per slot)</li>
 * </ul>
 *
 * <p>Uses L0MemoryAllocator for memory allocation, which is separate from the
 * MemoryManager-managed memory used by MainTable. L0 memory may be backed by
 * specialized hardware (CXL memory, PMEM) via JNI native methods.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class L0Table<K, N, S> implements AutoCloseable {

    // Bucket configuration
    private static final int BUCKET_SIZE = 64;           // 64 bytes per bucket
    private static final int BUCKET_SIZE_BITS = 6;       // 64 = 2^6, for fast multiplication via shift
    private static final int SLOTS_PER_BUCKET = 7;       // 7 data slots per bucket
    private static final int SLOT_SIZE = 8;              // 8 bytes per slot (1 long)
    private static final int EXTENSION_OFFSET = 56;      // Extension at offset 56 (after 7 slots)

    // Slot bit operations (unified with MainTable)
    private static final int HASH_SHIFT = 32;
    private static final long PTR_MASK = 0xFFFFFFFFL;

    // CLOCK algorithm: low 7 bits of extension long store accessed flags
    private static final long ACCESSED_MASK_ALL = 0x7FL; // bits 0-6

    // LFU strategy constants
    private static final int LFU_FREQ_BITS = 8;
    private static final int LFU_FREQ_MASK = 0xFF;

    // LRU strategy constants
    private static final int LRU_TS_BITS = 9;
    private static final int LRU_TS_MASK = 0x1FF;

    // TinyLFU decay interval
    private static final long DECAY_INTERVAL = 10000L;

    /**
     * Replacement policies for L0 cache eviction.
     */
    public enum ReplacementPolicy {
        /** Clock algorithm (1-bit accessed flag, recommended default) */
        CLOCK,
        /** Least Recently Used (9-bit relative timestamps) */
        LRU,
        /** Least Frequently Used (8-bit frequency counters) */
        LFU,
        /** TinyLFU with periodic decay */
        TINY_LFU,
        /** Random sampling + LRU (lightweight) */
        SAMPLED_LRU
    }

    private final L0MemoryAllocator l0Allocator;
    private final int bucketCount;
    private final MemorySegment[] memorySegments;  // Array for O(1) access
    private final int segmentSizeBits;  // log2(segmentSize) for bit-shift division
    private final int segmentMask;       // segmentSize - 1 for bit-mask modulo
    private final List<MemorySegment> originalAllocation;  // Keep reference for release
    private final ReplacementPolicy replacementPolicy;
    private final Random random;  // For SAMPLED_LRU

    // LRU timestamp counter (cycles 0-511)
    private int currentTimestamp = 0;

    // TinyLFU decay tracking
    private long tinyLfuAccessCount = 0;

    // Statistics and metrics
    // [BENCHMARK_TEST] volatile for thread-safe reads by L0TableMetricsCollector
    private volatile long accessCount = 0;
    private volatile long hitCount = 0;
    private volatile long missCount = 0;
    private volatile long evictionCount = 0;

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

            // Initialize all buckets (zero out all memory)
            clearAllSlots();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate L0 Table memory", e);
        }
    }

    // Entry chunk constants (must match MainTable)  
    private static final int ENTRY_CHUNK_BITS = 16;
    private static final int ENTRY_CHUNK_MASK = (1 << ENTRY_CHUNK_BITS) - 1;

    /**
     * Gets an entry from the L0 cache using object comparison.
     * 
     * <p>This is the heap object store version that uses direct object comparison
     * for key/namespace matching.
     *
     * @param hash the pre-computed hash value (full 32 bits used)
     * @param key the key object
     * @param namespace the namespace object
     * @param entryChunks the entry chunks array from MainTable
     * @return the HeapStateEntry if found, null otherwise
     */
    public HeapStateEntry<K, N, S> get(int hash, K key, N namespace, HeapStateEntry<K, N, S>[][] entryChunks) {
        accessCount++;
        int bucketIndex = hash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Check all 7 slots
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) continue;  // Empty slot
            if ((int)(slot >>> HASH_SHIFT) != hash) continue;  // Hash mismatch
            
            int idx = (int) slot - 1;  // ptr - 1
            HeapStateEntry<K, N, S> entry = entryChunks[idx >> ENTRY_CHUNK_BITS][idx & ENTRY_CHUNK_MASK];
            if (entry != null && entry.key.equals(key) 
                    && (entry.namespace == namespace || entry.namespace.equals(namespace))) {
                hitCount++;
                // Update replacement algorithm metadata
                updateAccessMetadata(segment, bucketOffset, i);
                return entry;
            }
        }

        missCount++;
        return null;
    }

    /**
     * Puts an entry into the L0 cache.
     * 
     * <p>This method is only called when the entry is guaranteed NOT to be in L0:
     * <ul>
     *   <li>From MainTable.get(): entry was found in MainTable, meaning it was not in L0</li>
     *   <li>From MainTable.put(): entry is newly created, so not in L0</li>
     * </ul>
     * Therefore, no duplicate check is needed - just find an empty slot or evict.
     *
     * @param hash the pre-computed hash value (full 32 bits used)
     * @param ptr the HeapEntryStore pointer of the entry
     * @return 0 if new entry inserted, or evicted ptr if eviction occurred
     */
    public int put(int hash, int ptr) {
        int bucketIndex = hash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        // Encode new slot value
        long newSlot = ((long) hash << HASH_SHIFT) | (ptr & PTR_MASK);

        // Find first empty slot
        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) {
                // Found empty slot, insert here
                segment.putLong(bucketOffset + i * SLOT_SIZE, newSlot);
                initSlotMetadata(segment, bucketOffset, i);
                return 0;
            }
        }

        // No empty slot, need eviction
        int victimSlot = selectVictimSlot(segment, bucketOffset);
        long oldSlot = segment.getLong(bucketOffset + victimSlot * SLOT_SIZE);
        segment.putLong(bucketOffset + victimSlot * SLOT_SIZE, newSlot);
        // Initialize replacement algorithm metadata for new entry
        initSlotMetadata(segment, bucketOffset, victimSlot);
        evictionCount++;

        return (int) oldSlot;  // Return evicted pointer
    }

    /**
     * Removes an entry from the L0 cache by hash and ptr.
     * 
     * <p>Simply finds the slot with matching ptr and clears it.
     *
     * @param hash the pre-computed hash value
     * @param ptr the HeapEntryStore pointer of the entry to remove
     * @return the removed entry pointer if found, 0 otherwise
     */
    public int remove(int hash, int ptr) {
        int bucketIndex = hash & (bucketCount - 1);
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) continue;
            
            // Compare ptr directly (low 32 bits of slot)
            if ((int) slot == ptr) {
                // Clear the slot (set to 0)
                segment.putLong(bucketOffset + i * SLOT_SIZE, 0);
                return ptr;
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
        currentTimestamp = 0;
    }

    /**
     * Gets statistics about the L0 table.
     */
    public L0TableStats getStats() {
        return new L0TableStats(
            getEntryCount(),
            bucketCount * SLOTS_PER_BUCKET,
            accessCount,
            hitCount,
            missCount,
            evictionCount,
            getLoadFactor()
        );
    }

    // ==================== Helper methods ====================

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

    /**
     * Clears all buckets by zeroing out memory.
     * Each bucket is 64 bytes = 8 longs.
     */
    private void clearAllSlots() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            // Zero out all 8 longs in the bucket
            for (int i = 0; i < 8; i++) {
                segment.putLong(bucketOffset + i * 8, 0L);
            }
        }
    }

    // ==================== Replacement Algorithm Methods ====================

    /**
     * Updates metadata when a slot is accessed (hit or update).
     */
    private void updateAccessMetadata(MemorySegment segment, int bucketOffset, int slotIndex) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
        long newExtLong;

        switch (replacementPolicy) {
            case CLOCK:
            case SAMPLED_LRU:
                // Set accessed bit
                newExtLong = extLong | (1L << slotIndex);
                break;

            case LFU:
                // Increment frequency (saturate at 255)
                {
                    int shift = slotIndex * LFU_FREQ_BITS;
                    int freq = (int) ((extLong >>> shift) & LFU_FREQ_MASK);
                    if (freq < LFU_FREQ_MASK) {
                        newExtLong = (extLong & ~((long) LFU_FREQ_MASK << shift)) | ((long) (freq + 1) << shift);
                    } else {
                        newExtLong = extLong;
                    }
                }
                break;

            case TINY_LFU:
                // Increment frequency + check decay
                {
                    int shift = slotIndex * LFU_FREQ_BITS;
                    int freq = (int) ((extLong >>> shift) & LFU_FREQ_MASK);
                    if (freq < LFU_FREQ_MASK) {
                        newExtLong = (extLong & ~((long) LFU_FREQ_MASK << shift)) | ((long) (freq + 1) << shift);
                    } else {
                        newExtLong = extLong;
                    }
                    segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
                    // Check and perform decay
                    if (++tinyLfuAccessCount >= DECAY_INTERVAL) {
                        decayAllFrequencies();
                        tinyLfuAccessCount = 0;
                    }
                    return;
                }

            case LRU:
                // Update timestamp
                {
                    int shift = slotIndex * LRU_TS_BITS;
                    int ts = (currentTimestamp++) & LRU_TS_MASK;
                    newExtLong = (extLong & ~((long) LRU_TS_MASK << shift)) | ((long) ts << shift);
                }
                break;

            default:
                newExtLong = extLong;
        }

        segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
    }

    /**
     * Initializes metadata when a new entry is inserted into a slot.
     */
    private void initSlotMetadata(MemorySegment segment, int bucketOffset, int slotIndex) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
        long newExtLong;

        switch (replacementPolicy) {
            case CLOCK:
            case SAMPLED_LRU:
                // Clear accessed bit (new entry not yet accessed after insert)
                newExtLong = extLong & ~(1L << slotIndex);
                break;

            case LFU:
            case TINY_LFU:
                // Initialize frequency to 1
                {
                    int shift = slotIndex * LFU_FREQ_BITS;
                    newExtLong = (extLong & ~((long) LFU_FREQ_MASK << shift)) | (1L << shift);
                }
                break;

            case LRU:
                // Set current timestamp
                {
                    int shift = slotIndex * LRU_TS_BITS;
                    int ts = (currentTimestamp++) & LRU_TS_MASK;
                    newExtLong = (extLong & ~((long) LRU_TS_MASK << shift)) | ((long) ts << shift);
                }
                break;

            default:
                newExtLong = extLong;
        }

        segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
    }

    /**
     * Selects a victim slot for eviction based on replacement policy.
     */
    private int selectVictimSlot(MemorySegment segment, int bucketOffset) {
        switch (replacementPolicy) {
            case CLOCK:
                return selectClockVictim(segment, bucketOffset);
            case LRU:
                return selectLruVictim(segment, bucketOffset);
            case LFU:
            case TINY_LFU:
                return selectLfuVictim(segment, bucketOffset);
            case SAMPLED_LRU:
                return selectSampledLruVictim(segment, bucketOffset);
            default:
                return 0;
        }
    }

    /**
     * CLOCK: find empty or unaccessed slot, else clear all and return slot 0.
     */
    private int selectClockVictim(MemorySegment segment, int bucketOffset) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);

        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) return i;
            if ((extLong & (1L << i)) == 0) return i;
        }

        segment.putLong(bucketOffset + EXTENSION_OFFSET, extLong & ~ACCESSED_MASK_ALL);
        return 0;
    }

    /**
     * LRU: find slot with oldest timestamp.
     */
    private int selectLruVictim(MemorySegment segment, int bucketOffset) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
        int now = currentTimestamp & LRU_TS_MASK;
        int oldestSlot = 0;
        int oldestAge = -1;

        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) return i;

            int ts = (int) ((extLong >>> (i * LRU_TS_BITS)) & LRU_TS_MASK);
            int age = (now - ts) & LRU_TS_MASK;
            if (age > oldestAge) {
                oldestAge = age;
                oldestSlot = i;
            }
        }
        return oldestSlot;
    }

    /**
     * LFU: find slot with lowest frequency.
     */
    private int selectLfuVictim(MemorySegment segment, int bucketOffset) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);
        int minSlot = 0;
        int minFreq = 256;

        for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
            long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
            if (slot == 0) return i;

            int freq = (int) ((extLong >>> (i * LFU_FREQ_BITS)) & LFU_FREQ_MASK);
            if (freq < minFreq) {
                minFreq = freq;
                minSlot = i;
            }
        }
        return minSlot;
    }

    /**
     * SAMPLED_LRU: randomly sample 2 slots and pick the unaccessed one.
     */
    private int selectSampledLruVictim(MemorySegment segment, int bucketOffset) {
        long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);

        int s1 = random.nextInt(SLOTS_PER_BUCKET);
        int s2 = random.nextInt(SLOTS_PER_BUCKET);

        long slot1 = segment.getLong(bucketOffset + s1 * SLOT_SIZE);
        if (slot1 == 0) return s1;

        long slot2 = segment.getLong(bucketOffset + s2 * SLOT_SIZE);
        if (slot2 == 0) return s2;

        boolean acc1 = (extLong & (1L << s1)) != 0;
        boolean acc2 = (extLong & (1L << s2)) != 0;

        if (!acc1 && acc2) return s1;
        if (acc1 && !acc2) return s2;
        return s1;
    }

    /**
     * Decays all frequencies by half (for TinyLFU).
     */
    private void decayAllFrequencies() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            MemorySegment segment = getSegmentForBucket(bucket);
            int bucketOffset = getBucketOffsetInSegment(bucket);
            long extLong = segment.getLong(bucketOffset + EXTENSION_OFFSET);

            long newExtLong = 0;
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                int freq = (int) ((extLong >>> (i * LFU_FREQ_BITS)) & LFU_FREQ_MASK);
                newExtLong |= ((long) (freq >>> 1) << (i * LFU_FREQ_BITS));
            }
            segment.putLong(bucketOffset + EXTENSION_OFFSET, newExtLong);
        }
    }

    // ==================== Statistics and metrics ====================

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

    /**
     * Gets the replacement policy used by this L0 table.
     */
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
            
            for (int i = 0; i < SLOTS_PER_BUCKET; i++) {
                long slot = segment.getLong(bucketOffset + i * SLOT_SIZE);
                if (slot != 0) {
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

        public L0TableStats(int validSlots, int totalSlots, long accessCount, long hitCount,
                          long missCount, long evictionCount, double loadFactor) {
            this.validSlots = validSlots;
            this.totalSlots = totalSlots;
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
                "hitRate=%.3f, missRate=%.3f}",
                validSlots, totalSlots, accessCount, hitCount, missCount,
                evictionCount, loadFactor, hitRate, missRate
            );
        }
    }

}
