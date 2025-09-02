package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;

/**
 * Main Table implementation for ForL0 State Backend.
 * 64-byte aligned buckets with 6 slots + 4 extension pointers.
 * Supports tree-like expansion and global resize.
 */
public class MainTable implements AutoCloseable {

    private static final int BUCKET_SIZE = 64;
    private static final int SLOTS_PER_BUCKET = 6;
    private static final int SLOT_SIZE = 10;
    private static final int EXTENSION_POINTERS = 4;
    private static final int SLOT_TAG_OFFSET = 0;
    private static final int SLOT_POINTER_OFFSET = 2;
    private static final int EXTENSION_POINTERS_OFFSET = 60;
    private static final double DEFAULT_LOAD_FACTOR_THRESHOLD = 1.5;
    private static final int MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET = 255;
    private static final int MAX_EXTENSION_BUCKETS = 255;
    private static final byte NULL_BUCKET_ID = 0;

    private final MemoryManagerAllocator allocator;
    private int bucketCount;
    private List<MemorySegment> memorySegments;
    private int[] extensionBucketCounts;
    private final int maxExtensionBuckets;
    private final boolean[] extensionBucketInUse;
    private int nextFreeExtensionBucket;
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    private int maxExtensionBucketsUsed = 0;

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this(allocator, bucketCountPow2, DEFAULT_LOAD_FACTOR_THRESHOLD);
    }

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2, double loadFactorThreshold) {
        this.allocator = allocator;
        this.bucketCount = 1 << bucketCountPow2;
        this.loadFactorThreshold = loadFactorThreshold;
        this.maxExtensionBuckets = MAX_EXTENSION_BUCKETS;
        this.extensionBucketInUse = new boolean[MAX_EXTENSION_BUCKETS + 1];
        this.nextFreeExtensionBucket = 1;
        try {
            long totalSize = (long) (bucketCount + maxExtensionBuckets) * BUCKET_SIZE;
            this.memorySegments = allocator.allocate((int) totalSize);
            clearAllSlots();
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
        this.extensionBucketCounts = new int[bucketCount];
    }

    public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long result = searchBucketSlots(bucketIndex, tag, kb, klen, nb, nlen, arena);
        return result != 0 ? result : searchExtensionBuckets(bucketIndex, tag, kb, klen, nb, nlen, arena);
    }

    public long put(int keyHash, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long oldPtr = putInBucketSlots(bucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);
        if (oldPtr != -1) {
            if (oldPtr == 0) {
                totalEntries++;
                checkResizeNeeded();
            }
            return oldPtr;
        }

        long ext = putInExtensionBuckets(bucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);
        if (ext != -1) {
            if (ext == 0) {
                totalEntries++;
                checkResizeNeeded();
            }
            return ext;
        }

        needsResize = true;
        return ext;
    }

    public long remove(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long rem = removeFromBucketSlots(bucketIndex, tag, kb, klen, nb, nlen, arena);
        if (rem != 0) {
            totalEntries--;
            return rem;
        }

        long remExt = removeFromExtensionBuckets(bucketIndex, tag, kb, klen, nb, nlen, arena);
        if (remExt != 0) {
            totalEntries--;
        }
        return remExt;
    }

    // --- Core slot operations ---

    /**
     * Performs a put operation on bucket slots with unified logic.
     * @return 0 for new entry, positive for updated entry address, -1 for full bucket
     */
    private long putInSlots(MemorySegment segment, int bucketOffset, short tag, long entryAddress,
                           byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int empty = -1;

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (ptr == 0) {
                if (empty == -1) empty = slot;
                continue;
            }

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

    private long searchBucketSlots(int bucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

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

    private long putInBucketSlots(int bucketIndex, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return putInSlots(segment, bucketOffset, tag, entryAddress, kb, klen, nb, nlen, arena);
    }

    private long removeFromBucketSlots(int bucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

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

    // --- Extension bucket operations ---

    private long searchExtensionBuckets(int bucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (extId == 0) return 0;
        return searchInExtensionBucket(extId, tag, kb, klen, nb, nlen, arena);
    }

    private long putInExtensionBuckets(int bucketIndex, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extId == 0) {
            extId = allocateExtensionBucket();
            if (extId == NULL_BUCKET_ID) return -1;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extId);
            updateExtensionBucketCount(bucketIndex, 1);
        }

        return putInExtensionBucket(extId, tag, entryAddress, kb, klen, nb, nlen, arena);
    }

    private long removeFromExtensionBuckets(int bucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (extId == 0) return 0;

        long removed = removeFromExtensionBucket(extId, tag, kb, klen, nb, nlen, arena);
        if (removed != 0 && isExtensionBucketEmpty(extId)) {
            freeExtensionBucket(extId);
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, (byte)0);
            updateExtensionBucketCount(bucketIndex, -1);
        }
        return removed;
    }

    // --- Extension bucket implementation ---

    private long searchInExtensionBucket(byte bucketId, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return 0;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);

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

    private long putInExtensionBucket(byte bucketId, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return -1;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
        return putInSlots(segment, bucketOffset, tag, entryAddress, kb, klen, nb, nlen, arena);
    }

    private long removeFromExtensionBucket(byte bucketId, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return 0;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);

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

    private boolean isExtensionBucketEmpty(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return true;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) return false;
        }
        return true;
    }

    // --- Extension bucket management ---

    private byte allocateExtensionBucket() {
        for (int id = nextFreeExtensionBucket; id <= maxExtensionBuckets; id++) {
            if (!extensionBucketInUse[id]) {
                extensionBucketInUse[id] = true;
                updateNextFreeExtensionBucketId(id + 1);
                return (byte) id;
            }
        }

        for (int id = 1; id < nextFreeExtensionBucket; id++) {
            if (!extensionBucketInUse[id]) {
                extensionBucketInUse[id] = true;
                updateNextFreeExtensionBucketId(id + 1);
                return (byte) id;
            }
        }
        return NULL_BUCKET_ID;
    }

    private void freeExtensionBucket(byte bucketId) {
        int id = bucketId & 0xFF;
        if (id == 0 || id > maxExtensionBuckets) return;

        if (extensionBucketInUse[id]) {
            extensionBucketInUse[id] = false;
            if (id < nextFreeExtensionBucket) {
                nextFreeExtensionBucket = id;
            }
        }
    }

    private void updateNextFreeExtensionBucketId(int start) {
        for (int id = Math.max(1, start); id <= maxExtensionBuckets; id++) {
            if (!extensionBucketInUse[id]) {
                nextFreeExtensionBucket = id;
                return;
            }
        }
        nextFreeExtensionBucket = maxExtensionBuckets + 1;
    }

    // --- Public accessors ---

    public double getLoadFactor() {
        return bucketCount == 0 ? 0.0 : (double) totalEntries / bucketCount;
    }

    public boolean needsResize() {
        return needsResize;
    }

    public int getMaxExtensionBucketsUsed() {
        return maxExtensionBucketsUsed;
    }

    public TableStats getStats() {
        return new TableStats(bucketCount, totalEntries, getLoadFactor(),
                             getMaxExtensionBucketsUsed(), getAllocatedExtensionBuckets(), needsResize);
    }

    // --- Resize operations ---

    public boolean tryResize(EntryArena entryArena) {
        if (!needsResize) return false;
        resize(entryArena);
        return true;
    }

    public void forceResize(EntryArena entryArena) {
        needsResize = true;
        resize(entryArena);
    }

    public synchronized void resize(EntryArena entryArena) {
        if (!needsResize || entryArena == null) return;

        List<ResizeEntry> allEntries = collectAllEntries(entryArena);
        int newBucketCount = bucketCount * 2;
        long newTotalSize = (long) (newBucketCount + maxExtensionBuckets) * BUCKET_SIZE;

        List<MemorySegment> newMemorySegments;
        try {
            newMemorySegments = allocator.allocate((int) newTotalSize);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }

        int[] newExtensionBucketCounts = new int[newBucketCount];
        clearMemorySegments(newMemorySegments);
        clearExtensionBuckets();
        migrateEntriesToNewTable(allEntries, newMemorySegments, newBucketCount, newExtensionBucketCounts, entryArena);

        List<MemorySegment> oldSegments = this.memorySegments;
        this.memorySegments = newMemorySegments;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        needsResize = false;
        maxExtensionBucketsUsed = calculateMaxExtensionBuckets(newExtensionBucketCounts);

        allocator.release(oldSegments);
    }

    // --- Iteration support ---

    public void forEachEntry(EntryVisitor visitor) {
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            visitBucketSlots(bucketIndex, visitor);
            visitExtensionBuckets(bucketIndex, visitor);
        }
    }

    public void visitBucketSlots(byte bucketId, EntryVisitor visitor, int baseBucketIndex) {
        if (bucketId != NULL_BUCKET_ID) {
            visitExtensionBucket(bucketId, visitor, baseBucketIndex);
        }
    }

    // --- Helper methods ---

    private MemorySegment getSegmentForBucket(int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / memorySegments.get(0).size();
        return memorySegments.get(Math.min(segmentIndex, memorySegments.size() - 1));
    }

    private MemorySegment getSegmentForBucket(List<MemorySegment> segments, int bucketIndex) {
        if (segments.isEmpty()) {
            throw new IllegalStateException("No memory segments available");
        }
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segments.get(0).size();
        return segments.get(Math.min(segmentIndex, segments.size() - 1));
    }

    private int getBucketOffsetInSegment(int bucketIndex) {
        int segmentSize = memorySegments.get(0).size();
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    private int getBucketOffsetInSegment(int bucketIndex, int segmentSize) {
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    private void clearAllSlots() {
        for (MemorySegment segment : memorySegments) {
            for (int offset = 0; offset < segment.size(); offset += 8) {
                segment.putLong(offset, 0L);
            }
        }
    }

    private void clearMemorySegments(List<MemorySegment> segments) {
        for (MemorySegment segment : segments) {
            for (int offset = 0; offset < segment.size(); offset += 8) {
                segment.putLong(offset, 0L);
            }
        }
    }

    private void clearExtensionBuckets() {
        for (int i = 1; i <= maxExtensionBuckets; i++) {
            extensionBucketInUse[i] = false;
        }
        nextFreeExtensionBucket = 1;
    }

    private int getAllocatedExtensionBuckets() {
        int count = 0;
        for (int i = 1; i <= maxExtensionBuckets; i++) {
            if (extensionBucketInUse[i]) count++;
        }
        return count;
    }

    private void visitExtensionBucket(byte bucketId, EntryVisitor visitor, int bucketIndex) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                visitor.visit(slotPointer, bucketIndex, slotTag);
            }
        }
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
                visitExtensionBucket(extensionBucketId, visitor, bucketIndex);
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

    // --- Resize implementation ---

    private static class ResizeEntry {
        final long entryAddress;
        final int keyHash;
        final short tag;

        ResizeEntry(long entryAddress, int keyHash, short tag) {
            this.entryAddress = entryAddress;
            this.keyHash = keyHash;
            this.tag = tag;
        }
    }

    private List<ResizeEntry> collectAllEntries(EntryArena entryArena) {
        List<ResizeEntry> entries = new ArrayList<>();
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            collectBucketSlots(bucketIndex, entries, entryArena);
            collectExtensionBuckets(bucketIndex, entries, entryArena);
        }
        return entries;
    }

    private void collectBucketSlots(int bucketIndex, List<ResizeEntry> entries, EntryArena entryArena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                addResizeEntry(entryArena, entries, slotPointer);
            }
        }
    }

    private void collectExtensionBuckets(int bucketIndex, List<ResizeEntry> entries, EntryArena entryArena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int i = 0; i < EXTENSION_POINTERS; i++) {
            byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
            if (extensionBucketId != 0) {
                collectFromExtensionBucket(bucketIndex, extensionBucketId, entryAddress -> {
                    addResizeEntry(entryArena, entries, entryAddress);
                });
            }
        }
    }

    private void addResizeEntry(EntryArena entryArena, List<ResizeEntry> entries, long entryAddress) {
        if (entryArena == null) return;

        try {
            byte[] keyBytes = entryArena.getKeyBytes(entryAddress);
            byte[] nsBytes = entryArena.getNamespaceBytes(entryAddress);
            if (keyBytes == null || nsBytes == null) return;

            int fullHash = HashFunctions.jenkinsHashCombined(keyBytes, keyBytes.length, nsBytes, nsBytes.length);
            short tag = HashFunctions.murmur16(keyBytes, keyBytes.length, nsBytes, nsBytes.length);
            entries.add(new ResizeEntry(entryAddress, fullHash, tag));
        } catch (Exception ignore) {
        }
    }

    private void collectFromExtensionBucket(int bucketIndex, byte bucketId, java.util.function.Consumer<Long> collector) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return;

        int extensionBucketIndex = bucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                collector.accept(slotPointer);
            }
        }
    }

    private void migrateEntriesToNewTable(List<ResizeEntry> entries, List<MemorySegment> newMemorySegments,
                                          int newBucketCount, int[] newExtensionBucketCounts, EntryArena entryArena) {
        for (ResizeEntry entry : entries) {
            int newBucketIndex = entry.keyHash & (newBucketCount - 1);
            if (insertInNewBucketSlots(newMemorySegments, newBucketIndex, entry.tag, entry.entryAddress)) {
                continue;
            }
            insertInNewExtensionBucket(newMemorySegments, newExtensionBucketCounts, newBucketIndex, entry.tag, entry.entryAddress, entryArena);
        }
    }

    private boolean insertInNewBucketSlots(List<MemorySegment> newMemorySegments, int bucketIndex, short tag, long entryAddress) {
        MemorySegment segment = getSegmentForBucket(newMemorySegments, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, segment.size());

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer == 0) {
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return true;
            }
        }
        return false;
    }

    private boolean insertInNewExtensionBucket(List<MemorySegment> newMemorySegments, int[] newExtensionBucketCounts,
                                               int bucketIndex, short tag, long entryAddress, EntryArena entryArena) {
        int extensionIndex = tag & 0x3;
        MemorySegment segment = getSegmentForBucket(newMemorySegments, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, segment.size());

        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (extensionBucketId == 0) {
            extensionBucketId = allocateExtensionBucket();
            if (extensionBucketId == NULL_BUCKET_ID) return false;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extensionBucketId);
            newExtensionBucketCounts[bucketIndex]++;
        }

        byte[] keyBytes = entryArena.getKeyBytes(entryAddress);
        byte[] nsBytes = entryArena.getNamespaceBytes(entryAddress);
        if (keyBytes == null || nsBytes == null) return false;

        long result = putInNewTableExtensionBucket(newMemorySegments, extensionBucketId, tag, entryAddress, keyBytes, keyBytes.length, nsBytes, nsBytes.length, entryArena);
        return result != -1;
    }

    private long putInNewTableExtensionBucket(List<MemorySegment> newMemorySegments, byte bucketId, short tag, long entryAddress,
                                             byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int id = bucketId & 0xFF;
        if (id == 0 || !extensionBucketInUse[id]) return -1;

        int newBucketCount = bucketCount * 2;
        int extensionBucketIndex = newBucketCount + (id - 1);
        MemorySegment segment = getSegmentForBucket(newMemorySegments, extensionBucketIndex);
        int bucketOffset = getBucketOffsetInSegment(extensionBucketIndex, segment.size());

        return putInSlots(segment, bucketOffset, tag, entryAddress, kb, klen, nb, nlen, arena);
    }

    @FunctionalInterface
    public interface EntryVisitor {
        void visit(long entryAddress, int keyHash, short tag);
    }

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

    @Override
    public void close() throws Exception {
        if (allocator != null && memorySegments != null) {
            allocator.release(memorySegments);
        }
    }
}
