package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

/**
 * L0 Table implementation for hot key caching in ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 4 slots.
 * Each slot contains: tag(2B) + valid(1B) + extension(5B) + pointer(8B) = 16B
 */
public class L0Table implements AutoCloseable {

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    // L0 bucket and slot layout constants
    private static final int BUCKET_SIZE = 64;  // 64 bytes per bucket
    private static final int SLOTS_PER_BUCKET = 4;  // 4 slots per bucket
    private static final int SLOT_SIZE = 16;  // 16 bytes per slot

    // Slot field offsets within a slot
    private static final int SLOT_TAG_OFFSET = 0;      // 2 bytes
    private static final int SLOT_VALID_OFFSET = 2;    // 1 byte
    private static final int SLOT_EXTENSION_OFFSET = 3; // 5 bytes (for alignment and future use)
    private static final int SLOT_POINTER_OFFSET = 8;  // 8 bytes

    private final MemoryManagerAllocator allocator;
    private final int bucketCount;
    private final long baseAddress;
    private final long totalSize;

    /**
     * Creates an L0 Table with specified number of buckets.
     *
     * @param allocator Memory allocator for L0 region
     * @param bucketCountPow2 Number of buckets as power of 2 (e.g., 10 means 1024 buckets)
     */
    public L0Table(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this.allocator = allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.totalSize = (long) bucketCount * BUCKET_SIZE;

        try {
            // Allocate 64-byte aligned memory for L0 table
            this.baseAddress = allocator.allocateAligned(totalSize, BUCKET_SIZE);

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
     * @param kvMatcher Function to verify actual key match
     * @return Pointer to KVNode if found, 0 if not found
     */
    public long get(int keyHash, short tag, KVMatcher kvMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        // Check all 4 slots in the bucket
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            // Read slot fields
            byte valid = UNSAFE.getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) continue;  // Skip invalid slots

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            if (slotTag != tag) continue;  // Tag mismatch

            long pointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            // Verify actual key match using the matcher
            if (kvMatcher.matches(pointer)) {
                return pointer;
            }
        }

        return 0;  // Not found
    }

    /**
     * Puts a key-value entry into L0 table.
     * Uses configurable eviction policy when bucket is full.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param kvPointer Pointer to the KVNode
     * @return true if successfully inserted, false if eviction failed
     */
    public boolean put(int keyHash, short tag, long kvPointer) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        // First, try to find an empty slot or update existing entry
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            byte valid = UNSAFE.getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) {
                // Found empty slot, insert here
                writeSlot(slotAddress, tag, kvPointer);
                return true;
            }

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotTag == tag && slotPointer == kvPointer) {
                // Update existing entry (refresh access time if needed)
                updateSlotExtension(slotAddress);
                return true;
            }
        }

        // No empty slot found, need eviction
        int victimSlot = selectVictimSlot(bucketAddress);
        long victimAddress = bucketAddress + (long) victimSlot * SLOT_SIZE;

        writeSlot(victimAddress, tag, kvPointer);
        return true;
    }

    /**
     * Removes an entry from L0 table.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param kvPointer Pointer to verify exact match
     */
    public void remove(int keyHash, short tag, long kvPointer) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            byte valid = UNSAFE.getByte(slotAddress + SLOT_VALID_OFFSET);
            if (valid == 0) continue;

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotTag == tag && slotPointer == kvPointer) {
                // Mark slot as invalid
                UNSAFE.putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 0);
                return;
            }
        }
    }

    /**
     * Clears all entries in L0 table (used during resize operations).
     */
    public void clear() {
        clearAllSlots();
    }

    /**
     * Gets statistics about L0 table usage.
     */
    public L0TableStats getStats() {
        int validSlots = 0;
        int totalSlots = bucketCount * SLOTS_PER_BUCKET;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketAddress = baseAddress + (long) bucket * BUCKET_SIZE;

            for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
                long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
                byte valid = UNSAFE.getByte(slotAddress + SLOT_VALID_OFFSET);
                if (valid != 0) {
                    validSlots++;
                }
            }
        }

        return new L0TableStats(validSlots, totalSlots, bucketCount);
    }

    private void writeSlot(long slotAddress, short tag, long kvPointer) {
        UNSAFE.putShort(slotAddress + SLOT_TAG_OFFSET, tag);
        UNSAFE.putByte(slotAddress + SLOT_VALID_OFFSET, (byte) 1);
        UNSAFE.putLong(slotAddress + SLOT_POINTER_OFFSET, kvPointer);

        // Update extension field (could be used for LRU timestamp, etc.)
        updateSlotExtension(slotAddress);
    }

    private void updateSlotExtension(long slotAddress) {
        // For now, just update a simple counter or timestamp
        // This can be extended for LRU/LFU policies
        long currentTime = System.nanoTime();
        UNSAFE.putInt(slotAddress + SLOT_EXTENSION_OFFSET, (int) currentTime);
    }

    private int selectVictimSlot(long bucketAddress) {
        // Simple LRU-like policy: select slot with oldest extension value
        int victimSlot = 0;
        int oldestValue = Integer.MAX_VALUE;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
            int extensionValue = UNSAFE.getInt(slotAddress + SLOT_EXTENSION_OFFSET);

            if (extensionValue < oldestValue) {
                oldestValue = extensionValue;
                victimSlot = slot;
            }
        }

        return victimSlot;
    }

    private void clearAllSlots() {
        // Zero out all memory (makes all slots invalid)
        for (long offset = 0; offset < totalSize; offset += 8) {
            UNSAFE.putLong(baseAddress + offset, 0L);
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null && baseAddress != 0) {
            allocator.deallocate(baseAddress, totalSize);
        }
    }

    /**
     * Interface for key matching verification.
     */
    @FunctionalInterface
    public interface KVMatcher {
        boolean matches(long kvPointer);
    }

    /**
     * Statistics about L0 table usage.
     */
    public static class L0TableStats {
        public final int validSlots;
        public final int totalSlots;
        public final int bucketCount;
        public final double loadFactor;

        public L0TableStats(int validSlots, int totalSlots, int bucketCount) {
            this.validSlots = validSlots;
            this.totalSlots = totalSlots;
            this.bucketCount = bucketCount;
            this.loadFactor = (double) validSlots / totalSlots;
        }
    }
}
