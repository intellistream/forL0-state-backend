package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;

/**
 * Main Table implementation for ForL0 State Backend.
 * Each bucket is 64 bytes aligned and contains 6 slots + 4 extension pointers.
 * Supports local tree-like expansion for collision resolution.
 * Includes load factor monitoring and global resize triggering.
 * 
 * Uses MemorySegment operations instead of Unsafe for better safety and compatibility.
 */
public class MainTable implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MainTable.class);

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

    // 去掉final，允许resize后直接更新而不使用反射
    private MemoryManagerAllocator allocator; // 仍保持引用
    private int bucketCount;
    private List<MemorySegment> memorySegments;
    private ExtensionBucketPool extensionPool;
    private int[] extensionBucketCounts; // Track extension buckets per main bucket

    // Load monitoring
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    
    // Extension bucket tracking - avoid recalculating every time
    private int maxExtensionBucketsUsed = 0;

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
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            this.memorySegments = allocator.allocate((int) totalSize);
            this.extensionPool = new ExtensionBucketPool(allocator, 255);
            clearAllSlots();
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
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

    /**
     * Triggers a resize of the main table if needed.
     * This method is called by ForL0StateMap when resize conditions are met.
     *
     * @return true if resize was performed, false if not needed
     * @throws Exception if resize fails
     */
    public boolean tryResize(EntryArena entryArena) throws Exception {
        if (!needsResize) {
            return false;
        }

        resize(entryArena);
        return true;
    }

    /**
     * Forces a resize of the main table regardless of current conditions.
     * Used for testing and manual resize operations.
     *
     * @throws Exception if resize fails
     */
    public void forceResize(EntryArena entryArena) throws Exception {
        needsResize = true;
        resize(entryArena);
    }

    /**
     * Resizes the main table to double capacity.
     * This method performs a full rehash of all entries while preserving KVNode addresses.
     *
     * @throws Exception if resize fails due to memory allocation issues
     */
    public synchronized void resize(EntryArena entryArena) throws Exception {
        if (!needsResize) {
            return;
        }
        if (entryArena == null) {
            throw new IllegalStateException("EntryArena required for correct rehash during resize");
        }
        int oldBucketCount = bucketCount;
        LOG.debug("Starting MainTable resize from {} to {} buckets", oldBucketCount, oldBucketCount * 2);
        List<ResizeEntry> allEntries = collectAllEntries(entryArena);
        LOG.debug("Collected {} entries for migration", allEntries.size());
        int newBucketCount = oldBucketCount * 2;
        List<MemorySegment> newMemorySegments;
        ExtensionBucketPool newExtensionPool;
        int[] newExtensionBucketCounts;
        try {
            long newTotalSize = (long) newBucketCount * BUCKET_SIZE;
            if (newTotalSize > Integer.MAX_VALUE) {
                throw new Exception("Requested table size exceeds supported limit");
            }
            newMemorySegments = allocator.allocate((int) newTotalSize);
            newExtensionPool = new ExtensionBucketPool(allocator, 255);
            newExtensionBucketCounts = new int[newBucketCount];
            clearMemorySegments(newMemorySegments);
        } catch (Exception e) {
            LOG.error("Failed to allocate memory for resize", e);
            throw new Exception("Resize failed: could not allocate new table memory", e);
        }
        try {
            migrateEntriesToNewTable(allEntries, newMemorySegments, newExtensionPool, newBucketCount, newExtensionBucketCounts);
        } catch (Exception e) {
            try {
                newExtensionPool.close();
                allocator.release(newMemorySegments);
            } catch (Exception cleanupEx) {
                LOG.warn("Cleanup failure after migration error", cleanupEx);
            }
            throw new Exception("Resize failed during entry migration", e);
        }
        // 交换引用（不再使用反射）
        List<MemorySegment> oldSegments = this.memorySegments;
        ExtensionBucketPool oldPool = this.extensionPool;
        this.memorySegments = newMemorySegments;
        this.extensionPool = newExtensionPool;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        needsResize = false;
        maxExtensionBucketsUsed = calculateMaxExtensionBuckets(newExtensionBucketCounts);
        try {
            oldPool.close();
            allocator.release(oldSegments);
        } catch (Exception e) {
            LOG.warn("Failed to cleanup old table resources", e);
        }
        LOG.info("MainTable resize completed: {} -> {} buckets, {} entries, load factor: {}", oldBucketCount, newBucketCount, totalEntries, getLoadFactor());
    }

    /**
     * Data structure for holding entry information during resize.
     */
    private static class ResizeEntry {
        final long entryAddress;
        final int keyHash; // 真实hash
        final short tag;   // 重新计算的tag
        ResizeEntry(long entryAddress, int keyHash, short tag) {
            this.entryAddress = entryAddress;
            this.keyHash = keyHash;
            this.tag = tag;
        }
    }

    /**
     * Collects all existing entries from the current table for migration.
     */
    private List<ResizeEntry> collectAllEntries(EntryArena entryArena) {
        List<ResizeEntry> entries = new ArrayList<>();
        // Visit all buckets and their extensions
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            // Collect from main bucket slots
            visitBucketSlots(bucketIndex, (entryAddress, ignoredHash, ignoredTag) -> {
                addResizeEntry(entryArena, entries, entryAddress);
            });

            // Collect from extension buckets
            visitExtensionBuckets(bucketIndex, (entryAddress, ignoredHash, ignoredTag) -> {
                addResizeEntry(entryArena, entries, entryAddress);
            });
        }
        return entries;
    }

    private void addResizeEntry(EntryArena entryArena, List<ResizeEntry> entries, long entryAddress) {
        if (entryArena == null) {
            // 兼容：无法重算hash，跳过（理论上不会发生）
            return;
        }
        try {
            byte[] keyBytes = entryArena.getKeyBytes(entryAddress);
            byte[] nsBytes = entryArena.getNamespaceBytes(entryAddress);
            if (keyBytes == null || nsBytes == null) {
                return;
            }
            int keyHash = HashFunctions.murmurHash3(keyBytes);
            int nsHash = HashFunctions.murmurHash3(nsBytes);
            int fullHash = keyHash ^ nsHash;
            short tag = (short) (fullHash & 0xFFFF);
            entries.add(new ResizeEntry(entryAddress, fullHash, tag));
        } catch (Exception ignore) {
            // 忽略损坏条目
        }
    }

    // 调整迁移逻辑使用真实hash
    private void migrateEntriesToNewTable(List<ResizeEntry> entries,
                                          List<MemorySegment> newMemorySegments,
                                          ExtensionBucketPool newExtensionPool,
                                          int newBucketCount,
                                          int[] newExtensionBucketCounts) throws Exception {
        int migratedCount = 0;
        for (ResizeEntry entry : entries) {
            int newBucketIndex = entry.keyHash & (newBucketCount - 1);
            if (insertInNewBucketSlots(newMemorySegments, newBucketIndex, entry.tag, entry.entryAddress)) {
                migratedCount++;
                continue;
            }
            if (insertInNewExtensionBucket(newMemorySegments, newExtensionPool, newExtensionBucketCounts, newBucketIndex, entry.tag, entry.entryAddress)) {
                migratedCount++;
                continue;
            }
            throw new Exception("Failed to migrate entry during resize - new table unexpectedly full");
        }
        if (migratedCount != entries.size()) {
            throw new Exception(String.format("Migration incomplete: %d/%d entries migrated", migratedCount, entries.size()));
        }
    }

    /**
     * Inserts an entry into new table's main bucket slots.
     */
    private boolean insertInNewBucketSlots(List<MemorySegment> newMemorySegments,
                                         int bucketIndex, short tag, long entryAddress) {
        MemorySegment segment = getSegmentForBucket(newMemorySegments, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, segment.size());

        // Find empty slot
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (slotPointer == 0) {
                // Empty slot found, insert here
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return true;
            }
        }

        return false; // No empty slots
    }

    /**
     * Inserts an entry into new table's extension bucket.
     */
    private boolean insertInNewExtensionBucket(List<MemorySegment> newMemorySegments,
                                             ExtensionBucketPool newExtensionPool,
                                             int[] newExtensionBucketCounts,
                                             int bucketIndex, short tag, long entryAddress) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(newMemorySegments, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, segment.size());

        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extensionBucketId == 0) {
            // Allocate new extension bucket
            extensionBucketId = newExtensionPool.allocateBucket();
            if (extensionBucketId == 0) {
                return false; // Pool exhausted
            }
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extensionBucketId);
            newExtensionBucketCounts[bucketIndex]++;
        }

        // 迁移期间按 bucketId 插入
        EntryMatcher dummyMatcher = (addr) -> false;
        try {
            long result = newExtensionPool.putInBucket(extensionBucketId, tag, entryAddress, dummyMatcher);
            return result == 0;
        } catch (Exception e) {
            return false; // 迁移期间失败视为不可用
        }
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

        // 直接按 bucketId 插入
        return extensionPool.putInBucket(extensionBucketId, tag, entryAddress, entryMatcher);
    }

    /**
     * Public iteration over all entries for snapshotting/inspection.
     * This visits main bucket slots and all extension buckets.
     */
    public void forEachEntry(EntryVisitor visitor) {
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            visitBucketSlots(bucketIndex, visitor);
            visitExtensionBuckets(bucketIndex, visitor);
        }
    }

    /**
     * Helper methods for new table operations during resize.
     */
    private MemorySegment getSegmentForBucket(List<MemorySegment> segments, int bucketIndex) {
        if (segments.isEmpty()) {
            throw new IllegalStateException("No memory segments available");
        }
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segments.get(0).size();
        return segments.get(Math.min(segmentIndex, segments.size() - 1));
    }

    private void clearMemorySegments(List<MemorySegment> segments) {
        for (MemorySegment segment : segments) {
            for (int offset = 0; offset < segment.size(); offset += 8) {
                segment.putLong(offset, 0L);
            }
        }
    }

    private int calculateMaxExtensionBuckets(int[] extensionBucketCounts) {
        int max = 0;
        for (int count : extensionBucketCounts) {
            if (count > max) {
                max = count;
            }
        }
        return max;
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

        // 直接按 bucketId 查询，避免地址->ID 的线性扫描
        return extensionPool.searchInBucket(extensionBucketId, tag, entryMatcher);
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

        long removedPointer = extensionPool.removeFromBucket(extensionBucketId, tag, entryMatcher);

        // Check if extension bucket is now empty and can be freed
        if (removedPointer != 0 && extensionPool.isBucketEmpty(extensionBucketId)) {
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
                // 直接按 bucketId 访问
                extensionPool.visitBucketSlots(extensionBucketId, visitor, bucketIndex);
            }
        }
    }

    private void clearAllSlots() {
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

    private int getBucketOffsetInSegment(int bucketIndex, int segmentSize) {
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
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
