package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.List;
import java.util.Random;

/**
 * L0 Table implementation for hot key caching in ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 4 slots.
 * Each slot contains: tag(2B) + valid(1B) + extension(5B) + pointer(8B) = 16B
 * Supports configurable replacement algorithms (LRU, LFU, etc.) for cache management.
 *
 * Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
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

    // Replacement algorithm types
    public enum ReplacementPolicy {
        LRU,    // Least Recently Used
        LFU,    // Least Frequently Used
        FIFO,   // First In First Out
        RANDOM  // Random replacement
    }

    private final MemoryManagerAllocator allocator;
    private final int bucketCount;
    private final List<MemorySegment> memorySegments;
    private final ReplacementPolicy replacementPolicy;
    private final Random random;

    // Statistics and metrics
    private long accessCount = 0;
    private long hitCount = 0;
    private long missCount = 0;
    private long evictionCount = 0;

    // Global counter for LRU/FIFO ordering
    private int globalCounter = 0;

    /**
     * Creates an L0 Table with specified number of buckets and LRU replacement policy.
     *
     * @param allocator Memory allocator for L0 region
     * @param bucketCountPow2 Number of buckets as power of 2
     */
    public L0Table(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this(allocator, bucketCountPow2, ReplacementPolicy.LRU);
    }

    /**
     * Creates an L0 Table with specified number of buckets and replacement policy.
     *
     * @param allocator Memory allocator for L0 region
     * @param bucketCountPow2 Number of buckets as power of 2
     * @param replacementPolicy Replacement algorithm to use
     */
    public L0Table(MemoryManagerAllocator allocator, int bucketCountPow2, ReplacementPolicy replacementPolicy) {
        this.allocator = allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.replacementPolicy = replacementPolicy;
        this.random = (replacementPolicy == ReplacementPolicy.RANDOM) ? new Random() : null;

        try {
            // Allocate memory segments for L0 table using the dedicated L0 allocation interface
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            // Use allocateL0 instead of allocate for L0-specific memory allocation
            this.memorySegments = allocator.allocateL0((int) totalSize);

            // Initialize all slots as invalid
            clearAllSlots();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate L0 Table memory", e);
        }
    }

    /**
     * Gets a value from L0 table by key hash and tag.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryMatcher Function to verify actual entry match using EntryArena
     * @return Entry address if found, 0 if not found
     */
    public long get(int keyHash, short tag, EntryMatcher entryMatcher) {
        accessCount++;
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = getBucketAddress(bucketIndex);

        // Check all 4 slots in the bucket
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            // Read slot fields
            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) continue;  // Skip invalid slots

            short slotTag = getShort(slotAddress + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;  // Tag mismatch

            long pointer = getLong(slotAddress + SLOT_POINTER_OFFSET);

            // Verify actual entry match using the matcher
            if (entryMatcher.matches(pointer)) {
                hitCount++;
                // Update access information for replacement policy
                updateSlotOnAccess(slotAddress);
                return pointer;
            }
        }

        missCount++;
        return 0;  // Not found
    }

    /**
     * Puts a key-value entry into L0 table.
     * Uses configurable eviction policy when bucket is full.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryAddress Address of entry in EntryArena
     * @param entryMatcher Function to verify entry match for updates
     * @return Previous entry address if updated, 0 if newly inserted
     */
    public long put(int keyHash, short tag, long entryAddress, EntryMatcher entryMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = getBucketAddress(bucketIndex);

        // First, try to find an empty slot or update existing entry
        int emptySlot = -1;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) {
                // Found empty slot, remember it but continue checking for updates
                if (emptySlot == -1) {
                    emptySlot = slot;
                }
                continue;
            }

            short slotTag = getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotTag == tag && entryMatcher.matches(slotPointer)) {
                // Found existing entry, update it
                long oldAddress = slotPointer;
                putLong(slotAddress + SLOT_POINTER_OFFSET, entryAddress);
                updateSlotOnAccess(slotAddress);
                return oldAddress;
            }
        }

        // No existing entry found, use empty slot if available
        if (emptySlot != -1) {
            long slotAddress = bucketAddress + (long) emptySlot * SLOT_SIZE;
            writeSlot(slotAddress, tag, entryAddress);
            return 0;  // New insertion
        }

        // No empty slot found, need eviction
        int victimSlot = selectVictimSlot(bucketAddress);
        long victimAddress = bucketAddress + (long) victimSlot * SLOT_SIZE;

        // Get old entry address before overwriting
        long oldEntryAddress = getLong(victimAddress + SLOT_POINTER_OFFSET);

        writeSlot(victimAddress, tag, entryAddress);
        evictionCount++;

        return oldEntryAddress;  // Return evicted entry address
    }

    /**
     * Removes an entry from L0 table by key hash and tag.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param entryMatcher Function to verify actual entry match
     * @return Removed entry address, 0 if not found
     */
    public long remove(int keyHash, short tag, EntryMatcher entryMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = getBucketAddress(bucketIndex);

        // Check all 4 slots in the bucket
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) continue;  // Skip invalid slots

            short slotTag = getShort(slotAddress + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;  // Tag mismatch

            long pointer = getLong(slotAddress + SLOT_POINTER_OFFSET);

            // Verify actual entry match using the matcher
            if (entryMatcher.matches(pointer)) {
                // Mark slot as invalid
                putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 0);
                return pointer;
            }
        }

        return 0;  // Not found
    }

    /**
     * Removes an entry from L0 table by entry address.
     *
     * @param entryAddress Entry address to remove
     */
    public void removeByAddress(long entryAddress) {
        if (entryAddress == 0) return;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketAddress = getBucketAddress(bucket);

            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

                byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
                if (valid == 0) continue;

                long pointer = getLong(slotAddress + SLOT_POINTER_OFFSET);
                if (pointer == entryAddress) {
                    // Mark slot as invalid
                    putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 0);
                    return;
                }
            }
        }
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
        globalCounter = 0;
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
            long bucketAddress = getBucketAddress(bucket);

            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

                byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
                if (valid == 0) continue;

                long pointer = getLong(slotAddress + SLOT_POINTER_OFFSET);
                if (pointer >= minAddress && pointer < maxAddress) {
                    // Mark slot as invalid
                    putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 0);
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

    // Helper methods for memory access

    private long getBucketAddress(int bucketIndex) {
        long offset = (long) bucketIndex * BUCKET_SIZE;
        int pageSize = allocator.getPageSize();
        int segmentIndex = (int) (offset / pageSize);
        int segmentOffset = (int) (offset % pageSize);

        // 确保segmentIndex不超出memorySegments的范围
        if (segmentIndex >= memorySegments.size()) {
            throw new IllegalStateException("Segment index " + segmentIndex +
                " exceeds allocated segments " + memorySegments.size() +
                " for bucket " + bucketIndex + ", offset " + offset);
        }

        return (long) segmentIndex << 32 | segmentOffset;
    }

    private byte getByte(long address) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        return memorySegments.get(segmentIndex).get(offset);
    }

    private void putByte(long address, byte value) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        memorySegments.get(segmentIndex).put(offset, value);
    }

    private short getShort(long address) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        return memorySegments.get(segmentIndex).getShort(offset);
    }

    private void putShort(long address, short value) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        memorySegments.get(segmentIndex).putShort(offset, value);
    }

    private int getInt(long address) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        return memorySegments.get(segmentIndex).getInt(offset);
    }

    private void putInt(long address, int value) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        memorySegments.get(segmentIndex).putInt(offset, value);
    }

    private long getLong(long address) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        return memorySegments.get(segmentIndex).getLong(offset);
    }

    private void putLong(long address, long value) {
        int segmentIndex = (int) (address >>> 32);
        int offset = (int) address;
        memorySegments.get(segmentIndex).putLong(offset, value);
    }

    private void clearAllSlots() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketAddress = getBucketAddress(bucket);
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
                putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 0);
            }
        }
    }

    private void writeSlot(long slotAddress, short tag, long entryAddress) {
        putShort(slotAddress + SLOT_TAG_OFFSET, tag);
        putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 1);
        putLong(slotAddress + SLOT_POINTER_OFFSET, entryAddress);

        // Initialize extension data based on replacement policy
        switch (replacementPolicy) {
            case LRU:
            case FIFO:
                putInt(slotAddress + SLOT_EXTENSION_OFFSET, incrementGlobalCounter());
                break;
            case LFU:
                putInt(slotAddress + SLOT_EXTENSION_OFFSET, 1); // Initial frequency
                break;
            case RANDOM:
                // No extension data needed for random
                break;
        }
    }

    private void updateSlotOnAccess(long slotAddress) {
        switch (replacementPolicy) {
            case LRU:
                putInt(slotAddress + SLOT_EXTENSION_OFFSET, incrementGlobalCounter());
                break;
            case LFU:
                int freq = getInt(slotAddress + SLOT_EXTENSION_OFFSET);
                putInt(slotAddress + SLOT_EXTENSION_OFFSET, freq + 1);
                break;
            case FIFO:
            case RANDOM:
                // No update needed for FIFO and RANDOM
                break;
        }
    }

    private int incrementGlobalCounter() {
        return ++globalCounter;
    }

    private int selectVictimSlot(long bucketAddress) {
        switch (replacementPolicy) {
            case LRU:
                return selectLRUVictim(bucketAddress);
            case LFU:
                return selectLFUVictim(bucketAddress);
            case FIFO:
                return selectFIFOVictim(bucketAddress);
            case RANDOM:
                return selectRandomVictim();
            default:
                return 0; // Default to first slot
        }
    }

    private int selectLRUVictim(long bucketAddress) {
        int victimSlot = 0;
        int minCounter = Integer.MAX_VALUE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) return slot; // Use empty slot first

            int counter = getInt(slotAddress + SLOT_EXTENSION_OFFSET);
            if (counter < minCounter) {
                minCounter = counter;
                victimSlot = slot;
            }
        }
        return victimSlot;
    }

    private int selectLFUVictim(long bucketAddress) {
        int victimSlot = 0;
        int minFreq = Integer.MAX_VALUE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) return slot; // Use empty slot first

            int freq = getInt(slotAddress + SLOT_EXTENSION_OFFSET);
            if (freq < minFreq) {
                minFreq = freq;
                victimSlot = slot;
            }
        }
        return victimSlot;
    }

    private int selectFIFOVictim(long bucketAddress) {
        int victimSlot = 0;
        int minCounter = Integer.MAX_VALUE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
            byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) return slot; // Use empty slot first

            int counter = getInt(slotAddress + SLOT_EXTENSION_OFFSET);
            if (counter < minCounter) {
                minCounter = counter;
                victimSlot = slot;
            }
        }
        return victimSlot;
    }

    private int selectRandomVictim() {
        return random.nextInt(SLOTS_PER_BUCKET);
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
            long bucketAddress = getBucketAddress(bucket);
            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
                byte valid = getByte(slotAddress + SLOT_VALID_OFFSET);
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
            allocator.release(memorySegments);
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

    /**
     * Interface for matching entries in L0 table.
     * Used to verify that tag matches correspond to actual key matches.
     */
    @FunctionalInterface
    public interface EntryMatcher {
        boolean matches(long entryAddress);
    }
}
