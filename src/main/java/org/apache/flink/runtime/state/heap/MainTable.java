package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

/**
 * Main Table implementation for ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 6 slots + 4 extension pointers.
 * Supports local tree-like expansion for collision resolution.
 */
public class MainTable implements AutoCloseable {

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

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

    private final MemoryManagerAllocator allocator;
    private final int bucketCount;
    private final long baseAddress;
    private final long totalSize;

    // Extension bucket pool
    private final ExtensionBucketPool extensionPool;

    /**
     * Creates a Main Table with specified number of buckets.
     *
     * @param allocator Memory allocator
     * @param bucketCountPow2 Number of buckets as power of 2
     */
    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this.allocator = allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.totalSize = (long) bucketCount * BUCKET_SIZE;

        try {
            // Allocate 64-byte aligned memory for main table
            this.baseAddress = allocator.allocateAligned(totalSize, BUCKET_SIZE);

            // Initialize extension bucket pool
            this.extensionPool = new ExtensionBucketPool(allocator, bucketCount * 4); // Initial pool size

            // Clear all buckets
            clearAllBuckets();

        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
    }

    /**
     * Gets a value from main table by key hash and tag.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param kvMatcher Function to verify actual key match
     * @return Pointer to KVNode if found, 0 if not found
     */
    public long get(int keyHash, short tag, L0Table.KVMatcher kvMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        // First check main bucket slots
        long result = searchBucketSlots(bucketAddress, tag, kvMatcher);
        if (result != 0) {
            return result;
        }

        // Check extension buckets if main bucket is full
        return searchExtensionBuckets(bucketAddress, tag, kvMatcher);
    }

    /**
     * Puts a key-value entry into main table.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param kvPointer Pointer to the KVNode
     * @param kvMatcher Function to verify key match for updates
     * @return Previous pointer if updated, 0 if newly inserted
     */
    public long put(int keyHash, short tag, long kvPointer, L0Table.KVMatcher kvMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        // First try to update existing entry or find empty slot in main bucket
        long oldPointer = putInBucketSlots(bucketAddress, tag, kvPointer, kvMatcher);
        if (oldPointer != -1) {
            return oldPointer;  // Successfully handled in main bucket
        }

        // Main bucket is full, need extension bucket
        long result = putInExtensionBuckets(bucketAddress, keyHash, tag, kvPointer, kvMatcher);
        return result == -1 ? 0 : result; // Convert -1 to 0 for consistency
    }

    /**
     * Removes an entry from main table.
     *
     * @param keyHash Hash value of the key
     * @param tag Tag value for quick comparison
     * @param kvMatcher Function to verify actual key match
     * @return Pointer to removed KVNode if found, 0 if not found
     */
    public long remove(int keyHash, short tag, L0Table.KVMatcher kvMatcher) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long bucketAddress = baseAddress + (long) bucketIndex * BUCKET_SIZE;

        // First check main bucket slots
        long removedPointer = removeFromBucketSlots(bucketAddress, tag, kvMatcher);
        if (removedPointer != 0) {
            return removedPointer;
        }

        // Check extension buckets
        return removeFromExtensionBuckets(bucketAddress, tag, kvMatcher);
    }

    /**
     * Gets the load factor of the main table.
     */
    public double getLoadFactor() {
        int occupiedSlots = 0;
        int totalSlots = bucketCount * SLOTS_PER_BUCKET;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketAddress = baseAddress + (long) bucket * BUCKET_SIZE;
            occupiedSlots += countOccupiedSlotsInBucket(bucketAddress);
        }

        return (double) occupiedSlots / totalSlots;
    }

    /**
     * Creates a new main table with double capacity for resize operation.
     */
    public MainTable createExpandedTable() {
        int newBucketCountPow2 = Integer.numberOfTrailingZeros(bucketCount) + 1;
        return new MainTable(allocator, newBucketCountPow2);
    }

    /**
     * Iterates through all valid entries in the main table.
     */
    public void forEachEntry(EntryVisitor visitor) {
        // Iterate main buckets
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            long bucketAddress = baseAddress + (long) bucket * BUCKET_SIZE;

            // Visit main bucket slots
            visitBucketSlots(bucketAddress, visitor);

            // Visit extension buckets
            visitExtensionBuckets(bucketAddress, visitor);
        }
    }

    private long searchBucketSlots(long bucketAddress, short tag, L0Table.KVMatcher kvMatcher) {
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) continue;  // Empty slot
            if (slotTag != tag) continue;    // Tag mismatch

            if (kvMatcher.matches(slotPointer)) {
                return slotPointer;
            }
        }
        return 0;
    }

    private long searchExtensionBuckets(long bucketAddress, short tag, L0Table.KVMatcher kvMatcher) {
        int extensionIndex = tag & 0x3;  // Use tag's low 2 bits to select extension
        byte extensionBucketId = UNSAFE.getByte(bucketAddress + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            return 0;  // No extension bucket
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        return searchBucketSlots(extensionBucketAddress, tag, kvMatcher);
    }

    private long putInBucketSlots(long bucketAddress, short tag, long kvPointer, L0Table.KVMatcher kvMatcher) {
        int emptySlot = -1;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                if (emptySlot == -1) {
                    emptySlot = slot;
                }
                continue;
            }

            if (slotTag == tag && kvMatcher.matches(slotPointer)) {
                // Update existing entry
                long oldPointer = slotPointer;
                UNSAFE.putLong(slotAddress + SLOT_POINTER_OFFSET, kvPointer);
                return oldPointer;
            }
        }

        // Insert in empty slot if available
        if (emptySlot != -1) {
            long slotAddress = bucketAddress + (long) emptySlot * SLOT_SIZE;
            UNSAFE.putShort(slotAddress + SLOT_TAG_OFFSET, tag);
            UNSAFE.putLong(slotAddress + SLOT_POINTER_OFFSET, kvPointer);
            return 0;  // New insertion
        }

        return -1;  // Bucket is full
    }

    private long putInExtensionBuckets(long bucketAddress, int keyHash, short tag, long kvPointer, L0Table.KVMatcher kvMatcher) {
        int extensionIndex = tag & 0x3;
        byte extensionBucketId = UNSAFE.getByte(bucketAddress + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            // Allocate new extension bucket
            extensionBucketId = extensionPool.allocateBucket();
            if (extensionBucketId == 0) {
                throw new RuntimeException("Failed to allocate extension bucket - pool exhausted");
            }
            UNSAFE.putByte(bucketAddress + EXTENSION_POINTERS_OFFSET + extensionIndex, extensionBucketId);
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        long result = putInBucketSlots(extensionBucketAddress, tag, kvPointer, kvMatcher);

        // If extension bucket is also full, we need to handle this case
        if (result == -1) {
            // For now, we'll throw an exception since we can't handle infinite extension
            throw new RuntimeException("Extension bucket is full - cannot insert more entries");
        }

        return result;
    }

    private long removeFromBucketSlots(long bucketAddress, short tag, L0Table.KVMatcher kvMatcher) {
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) continue;
            if (slotTag != tag) continue;

            if (kvMatcher.matches(slotPointer)) {
                // Clear the slot
                UNSAFE.putShort(slotAddress + SLOT_TAG_OFFSET, (short) 0);
                UNSAFE.putLong(slotAddress + SLOT_POINTER_OFFSET, 0L);
                return slotPointer;
            }
        }
        return 0;
    }

    private long removeFromExtensionBuckets(long bucketAddress, short tag, L0Table.KVMatcher kvMatcher) {
        int extensionIndex = tag & 0x3;
        byte extensionBucketId = UNSAFE.getByte(bucketAddress + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            return 0;  // No extension bucket
        }

        long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
        long removedPointer = removeFromBucketSlots(extensionBucketAddress, tag, kvMatcher);

        // Check if extension bucket is now empty and can be freed
        if (removedPointer != 0 && countOccupiedSlotsInBucket(extensionBucketAddress) == 0) {
            extensionPool.freeBucket(extensionBucketId);
            UNSAFE.putByte(bucketAddress + EXTENSION_POINTERS_OFFSET + extensionIndex, (byte) 0);
        }

        return removedPointer;
    }

    private int countOccupiedSlotsInBucket(long bucketAddress) {
        int count = 0;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                count++;
            }
        }
        return count;
    }

    private void visitBucketSlots(long bucketAddress, EntryVisitor visitor) {
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            long slotAddress = bucketAddress + (long) slot * SLOT_SIZE;

            short slotTag = UNSAFE.getShort(slotAddress + SLOT_TAG_OFFSET);
            long slotPointer = UNSAFE.getLong(slotAddress + SLOT_POINTER_OFFSET);

            if (slotPointer != 0) {
                visitor.visit(slotTag, slotPointer);
            }
        }
    }

    private void visitExtensionBuckets(long bucketAddress, EntryVisitor visitor) {
        for (int i = 0; i < EXTENSION_POINTERS; i++) {
            byte extensionBucketId = UNSAFE.getByte(bucketAddress + EXTENSION_POINTERS_OFFSET + i);
            if (extensionBucketId != 0) {
                long extensionBucketAddress = extensionPool.getBucketAddress(extensionBucketId);
                visitBucketSlots(extensionBucketAddress, visitor);
            }
        }
    }

    private void clearAllBuckets() {
        // Zero out all memory
        for (long offset = 0; offset < totalSize; offset += 8) {
            UNSAFE.putLong(baseAddress + offset, 0L);
        }
    }

    @Override
    public void close() throws Exception {
        if (extensionPool != null) {
            extensionPool.close();
        }
        if (allocator != null && baseAddress != 0) {
            allocator.deallocate(baseAddress, totalSize);
        }
    }

    /**
     * Interface for visiting entries during iteration.
     */
    @FunctionalInterface
    public interface EntryVisitor {
        void visit(short tag, long kvPointer);
    }
}
