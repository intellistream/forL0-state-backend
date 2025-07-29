package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

/**
 * Extension Bucket Pool for managing extension buckets in Main Table.
 * Supports up to 255 extension buckets per main bucket.
 * Single-threaded implementation as Flink Task state access is single-threaded.
 */
public class ExtensionBucketPool implements AutoCloseable {

    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();
    private static final int BUCKET_SIZE = 64;  // Same as main bucket size
    private static final byte NULL_BUCKET_ID = 0;

    private final MemoryManagerAllocator allocator;
    private final int maxBuckets;
    private final long baseAddress;
    private final long totalSize;

    // Free list for bucket allocation (single-threaded)
    private byte nextFreeBucketId;
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
            // Allocate 64-byte aligned memory for extension buckets
            this.baseAddress = allocator.allocateAligned(totalSize, BUCKET_SIZE);

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
        // Find next available bucket
        for (int id = nextFreeBucketId; id <= maxBuckets; id++) {
            if (!bucketInUse[id]) {
                bucketInUse[id] = true;

                // Clear the allocated bucket
                clearBucket((byte) id);

                // Update next free bucket hint
                updateNextFreeBucketId();

                return (byte) id;
            }
        }

        // Wrap around and search from beginning
        for (int id = 1; id < nextFreeBucketId; id++) {
            if (!bucketInUse[id]) {
                bucketInUse[id] = true;

                // Clear the allocated bucket
                clearBucket((byte) id);

                // Update next free bucket hint
                updateNextFreeBucketId();

                return (byte) id;
            }
        }

        return NULL_BUCKET_ID; // Pool exhausted
    }

    /**
     * Frees an extension bucket.
     *
     * @param bucketId Bucket ID to free
     */
    public void freeBucket(byte bucketId) {
        if (bucketId == NULL_BUCKET_ID || bucketId > maxBuckets) {
            return; // Invalid bucket ID
        }

        if (bucketInUse[bucketId]) {
            bucketInUse[bucketId] = false;

            // Clear the freed bucket
            clearBucket(bucketId);

            // Update free bucket hint
            if (bucketId < nextFreeBucketId) {
                nextFreeBucketId = bucketId;
            }
        }
    }

    /**
     * Gets the memory address of an extension bucket.
     *
     * @param bucketId Bucket ID (1-255)
     * @return Memory address of the bucket
     */
    public long getBucketAddress(byte bucketId) {
        if (bucketId == NULL_BUCKET_ID || bucketId > maxBuckets) {
            throw new IllegalArgumentException("Invalid bucket ID: " + bucketId);
        }

        // Bucket IDs start from 1, so subtract 1 for array indexing
        return baseAddress + (long) (bucketId - 1) * BUCKET_SIZE;
    }

    /**
     * Checks if a bucket is currently in use.
     *
     * @param bucketId Bucket ID to check
     * @return true if bucket is in use, false otherwise
     */
    public boolean isBucketInUse(byte bucketId) {
        if (bucketId == NULL_BUCKET_ID || bucketId > maxBuckets || bucketId < 0) {
            return false;
        }
        return bucketInUse[bucketId];
    }

    /**
     * Gets statistics about pool usage.
     */
    public PoolStats getStats() {
        int bucketsInUse = 0;
        for (int i = 1; i <= maxBuckets; i++) {
            if (bucketInUse[i]) {
                bucketsInUse++;
            }
        }

        return new PoolStats(bucketsInUse, maxBuckets);
    }

    /**
     * Resizes the pool to accommodate more buckets.
     * This is typically called during main table expansion.
     */
    public ExtensionBucketPool resize(int newMaxBuckets) {
        if (newMaxBuckets <= maxBuckets) {
            return this; // No need to resize
        }

        // Create new larger pool
        ExtensionBucketPool newPool = new ExtensionBucketPool(allocator, newMaxBuckets);

        // Copy existing buckets that are in use
        for (int id = 1; id <= maxBuckets; id++) {
            if (bucketInUse[id]) {
                byte newBucketId = newPool.allocateBucket();
                if (newBucketId != NULL_BUCKET_ID) {
                    // Copy bucket data
                    long oldAddress = getBucketAddress((byte) id);
                    long newAddress = newPool.getBucketAddress(newBucketId);

                    for (int offset = 0; offset < BUCKET_SIZE; offset += 8) {
                        long data = UNSAFE.getLong(oldAddress + offset);
                        UNSAFE.putLong(newAddress + offset, data);
                    }
                }
            }
        }

        return newPool;
    }

    private void updateNextFreeBucketId() {
        // Find next free bucket for faster allocation
        for (int id = nextFreeBucketId; id <= maxBuckets; id++) {
            if (!bucketInUse[id]) {
                nextFreeBucketId = (byte) id;
                return;
            }
        }

        // If no free bucket found after current position, wrap around
        for (int id = 1; id < nextFreeBucketId; id++) {
            if (!bucketInUse[id]) {
                nextFreeBucketId = (byte) id;
                return;
            }
        }

        // Pool might be full, keep current hint
    }

    private void clearBucket(byte bucketId) {
        long bucketAddress = getBucketAddress(bucketId);

        // Zero out the bucket
        for (int offset = 0; offset < BUCKET_SIZE; offset += 8) {
            UNSAFE.putLong(bucketAddress + offset, 0L);
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
        if (allocator != null && baseAddress != 0) {
            allocator.deallocate(baseAddress, totalSize);
        }
    }

    /**
     * Statistics about extension bucket pool usage.
     */
    public static class PoolStats {
        public final int bucketsInUse;
        public final int maxBuckets;
        public final double utilization;

        public PoolStats(int bucketsInUse, int maxBuckets) {
            this.bucketsInUse = bucketsInUse;
            this.maxBuckets = maxBuckets;
            this.utilization = maxBuckets > 0 ? (double) bucketsInUse / maxBuckets : 0.0;
        }

        @Override
        public String toString() {
            return String.format("ExtensionPool[%d/%d buckets, %.2f%% utilization]",
                    bucketsInUse, maxBuckets, utilization * 100);
        }
    }
}
