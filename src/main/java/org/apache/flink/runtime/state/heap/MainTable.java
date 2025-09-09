package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.Arrays;
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

    private MemorySegment lastFoundSegment = null;
    private int lastFountSlotOffset = -1;

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
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return searchBucketTree(tag, kb, klen, nb, nlen, arena, segment, bucketOffset);
    }

    /**
     * 插入或更新条目
     * @return 0 for new entry, positive for existing entry address, -1 for full (needs resize)
     */
    public long put(int keyHash, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long result = putInBucketTree(bucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);

        if (result == 0) {
            totalEntries++;
            checkResizeNeeded();
        } else if (result == -1) {
            needsResize = true;
        }

        return result;
    }

    public long remove(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long removed = removeFromBucketTree(tag, kb, klen, nb, nlen, arena, segment, bucketOffset);
        if (removed > 0) {
            totalEntries--;
        }
        return removed;
    }

    public void setSlotPointer(long entryAddress) {
        if (lastFoundSegment != null && lastFountSlotOffset != -1) {
            lastFoundSegment.putLong(lastFountSlotOffset + SLOT_POINTER_OFFSET, entryAddress);
            lastFoundSegment = null;
            lastFountSlotOffset = -1;
        } else {
            throw new IllegalStateException("No slot found to set pointer");
        }
    }

    /**
     * 递归搜索桶树，体现桶的统一结构
     */
    private long searchBucketTree(short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena,
                                  MemorySegment segment, int bucketOffset) {
        // 先搜索当前桶的槽位
        long result = searchBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, arena);
        if (result != 0) return result;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (extensionBucketId != 0) {
            int extensionBucketIndex = bucketCount + (extensionBucketId & 0xFF) - 1;
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            return searchBucketTree(tag, kb, klen, nb, nlen, arena, extensionSegment, extensionBucketOffset);
        }
        return 0;
    }

    private long searchBucketSlots(MemorySegment segment, int bucketOffset, short tag, byte[] kb, int klen, byte[] nb,
                                   int nlen, EntryArena arena) {
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

    /**
     * 递归插入到桶树，优先填充当前桶，满了再尝试扩展桶
     * @return 0 for new entry, positive for updated entry address, -1 for full (needs resize)
     */
    private long putInBucketTree(int bucketIndex, short tag, long entryAddress, byte[] kb, int klen, byte[] nb,
                                 int nlen, EntryArena arena) {
        // 先尝试插入当前桶
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long result = putInSlots(segment, bucketOffset, tag, entryAddress, kb, klen, nb, nlen, arena);
        if (result != -1) return result;

        // 当前桶满，根据tag确定扩展桶
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extId == 0) {
            // 分配新的扩展桶
            extId = allocateExtensionBucket();
            if (extId == NULL_BUCKET_ID) return -1;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extId);
            // 只有当前桶是基桶时才更新扩展桶计数
            if (bucketIndex < bucketCount) {
                extensionBucketCounts[bucketIndex] += 1;
                if (extensionBucketCounts[bucketIndex] > maxExtensionBucketsUsed) {
                    maxExtensionBucketsUsed = extensionBucketCounts[bucketIndex];
                }
            }
        }

        // 递归插入到扩展桶
        int extensionBucketIndex = bucketCount + (extId & 0xFF) - 1;
        return putInBucketTree(extensionBucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);
    }

    /**
     * Performs a put operation on bucket slots with unified logic.
     * @return 0 for new entry, positive for existing entry address, -1 for full bucket
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
                lastFoundSegment = segment;
                lastFountSlotOffset = slotOffset;
                // If entryAddress is 0, it's an in-place update (no need to change the pointer)
                if (entryAddress > 0) segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return ptr; // update
            }
        }

        if (empty != -1) {
            int slotOffset = bucketOffset + empty * SLOT_SIZE;
            lastFoundSegment = segment;
            lastFountSlotOffset = slotOffset;
            segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
            // Only set pointer if entryAddress is non-zero since it's already zeroed
            if (entryAddress > 0) segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
            return 0; // new
        }
        return -1; // full
    }

    /**
     * 递归从桶树中删除条目，不释放空扩展桶
     */
    private long removeFromBucketTree(short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena,
                                      MemorySegment segment, int bucketOffset) {
        // 先尝试从当前桶删除
        long removed = removeFromBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, arena);
        if (removed != 0) return removed;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (extensionBucketId != 0) {
            int extensionBucketIndex = bucketCount + (extensionBucketId & 0xFF) - 1;
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            removed = removeFromBucketTree(tag, kb, klen, nb, nlen, arena, extensionSegment, extensionBucketOffset);
        }
        return removed;
    }

    private long removeFromBucketSlots(MemorySegment segment, int bucketOffset, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
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

    // --- Iteration support ---

    public void forEachEntry(EntryVisitor visitor) {
        // 遍历所有主桶
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            visitBucketTree(bucketIndex, visitor);
        }
    }

    /**
     * 统一的桶树遍历方法，递归遍历桶及其所有扩展桶
     */
    private void visitBucketTree(int bucketIndex, EntryVisitor visitor) {
        // 访问当前桶的所有槽位
        visitBucketSlots(bucketIndex, visitor);

        // 如果是主桶，递归访问其扩展桶
        if (bucketIndex < bucketCount) {
            MemorySegment segment = getSegmentForBucket(bucketIndex);
            int bucketOffset = getBucketOffsetInSegment(bucketIndex);

            for (int i = 0; i < EXTENSION_POINTERS; i++) {
                byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
                if (extensionBucketId != 0) {
                    int id = extensionBucketId & 0xFF;
                    if (extensionBucketInUse[id]) {
                        int extensionBucketIndex = bucketCount + (id - 1);
                        visitBucketTree(extensionBucketIndex, visitor);
                    }
                }
            }
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

    // --- Extension bucket management ---

    private byte allocateExtensionBucket() {
        for (int i = 0; i < maxExtensionBuckets; i++) {
            int id = (nextFreeExtensionBucket - 1 + i) % maxExtensionBuckets + 1;
            if (!extensionBucketInUse[id]) {
                extensionBucketInUse[id] = true;

                // 更新 nextFreeExtensionBucket
                for (int nextId = id + 1; nextId <= maxExtensionBuckets; nextId++) {
                    if (!extensionBucketInUse[nextId]) {
                        nextFreeExtensionBucket = nextId;
                return (byte) id;
            }
        }
                nextFreeExtensionBucket = maxExtensionBuckets + 1;
                return (byte) id;
            }
        }
        return NULL_BUCKET_ID;
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

    public void tryResize(EntryArena entryArena) {
        if (!needsResize) return;
        resize(entryArena);
    }

    public void resize(EntryArena entryArena) {
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

        // 直接迁移条目，无需中间集合
        migrateAllEntriesToNewTable(newMemorySegments, newBucketCount, newExtensionBucketCounts, entryArena);

        List<MemorySegment> oldSegments = this.memorySegments;
        this.memorySegments = newMemorySegments;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        needsResize = false;
        maxExtensionBucketsUsed = Arrays.stream(extensionBucketCounts).max().orElse(0);

        allocator.release(oldSegments);
    }

    private void checkResizeNeeded() {
        if (getLoadFactor() >= loadFactorThreshold || getMaxExtensionBucketsUsed() >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            needsResize = true;
        }
    }

    // --- Resize implementation ---

    /**
     * 优化的直接迁移方法，消除中间数据结构
     * 在遍历过程中直接将条目插入新表，避免额外的内存分配
     */
    private void migrateAllEntriesToNewTable(List<MemorySegment> newMemorySegments, int newBucketCount,
                                             int[] newExtensionBucketCounts, EntryArena entryArena) {
        // 遍历所有基桶，每个基桶会递归遍历其扩展子树
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            migrateBucketTree(bucketIndex, newMemorySegments, newBucketCount, newExtensionBucketCounts, entryArena);
        }
    }

    /**
     * 递归迁移一个桶及其所有扩展桶的条目
     * 体现桶的树形结构：基桶是根节点，扩展桶是子节点
     */
    private void migrateBucketTree(int bucketIndex, List<MemorySegment> newMemorySegments, int newBucketCount,
                                   int[] newExtensionBucketCounts, EntryArena entryArena) {
        // 迁移当前桶的所有槽位数据
        migrateBucketSlots(bucketIndex, newMemorySegments, newBucketCount, newExtensionBucketCounts, entryArena);

        // 如果是基桶，递归迁移所有扩展桶的数据
        if (bucketIndex < bucketCount) {
            MemorySegment segment = getSegmentForBucket(bucketIndex);
            int bucketOffset = getBucketOffsetInSegment(bucketIndex);

            // 遍历4个扩展桶指针
            for (int i = 0; i < EXTENSION_POINTERS; i++) {
                byte extensionBucketId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
                if (extensionBucketId != 0) {
                    // 递归迁移扩展桶及其可能的子扩展桶
                    int extensionBucketIndex = bucketCount + (extensionBucketId & 0xFF) - 1;
                    migrateBucketTree(extensionBucketIndex, newMemorySegments, newBucketCount, newExtensionBucketCounts, entryArena);
                }
            }
        }
    }

    private void migrateBucketSlots(int bucketIndex, List<MemorySegment> newMemorySegments, int newBucketCount,
                                    int[] newExtensionBucketCounts, EntryArena entryArena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long entryAddress = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (entryAddress == 0) continue;

            short tag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);

            // 获取key和namespace信息用于重新哈希
            byte[] keyBytes = entryArena.getKeyBytes(entryAddress);
            byte[] nsBytes = entryArena.getNamespaceBytes(entryAddress);
            if (keyBytes == null || nsBytes == null) continue;

            // 重新计算哈希值并确定新桶位置
            int fullHash = HashFunctions.jenkinsHashCombined(keyBytes, keyBytes.length, nsBytes, nsBytes.length);
            int newBucketIndex = fullHash & (newBucketCount - 1);

            // 复用现有的递归插入逻辑，支持多级扩展桶
            putInNewTable(newMemorySegments, newBucketCount, newBucketIndex, tag, entryAddress, newExtensionBucketCounts);
        }
    }

    private void putInNewTable(List<MemorySegment> newMemorySegments, int newBucketCount, int bucketIndex,
                              short tag, long entryAddress, int[] newExtensionBucketCounts) {
        // 先尝试插入当前桶
        MemorySegment segment = getSegmentForBucket(newMemorySegments, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, segment.size());

        // 查找空槽位直接插入
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (ptr == 0) {
                // 找到空槽位，直接插入（新表中不需要检查重复）
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return;
            }
        }

        // 当前桶满，根据tag确定扩展桶 - 复用putInBucketTree的逻辑
        int extensionIndex = tag & 0x3;
        byte extId = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (extId == 0) {
            // 分配新的扩展桶
            extId = allocateExtensionBucket();
            if (extId == NULL_BUCKET_ID) return; // 无法分配，跳过
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extId);

            // 更新扩展桶计数（只对基桶计数）
            if (bucketIndex < newBucketCount) {
                newExtensionBucketCounts[bucketIndex]++;
            }
        }

        // 递归插入到扩展桶
        int extensionBucketIndex = newBucketCount + (extId & 0xFF) - 1;
        putInNewTable(newMemorySegments, newBucketCount, extensionBucketIndex, tag, entryAddress, newExtensionBucketCounts);
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
        // Use batch operations with reusable buffer for better performance
        clearSegments(memorySegments);
    }

    private void clearSegments(List<MemorySegment> memorySegments) {
        byte[] zeroArray = new byte[1024]; // Reusable zero buffer
        for (MemorySegment segment : memorySegments) {
            int remaining = segment.size();
            int offset = 0;
            while (remaining > 0) {
                int batchSize = Math.min(remaining, zeroArray.length);
                segment.put(offset, zeroArray, 0, batchSize);
                offset += batchSize;
                remaining -= batchSize;
            }
        }
    }

    private void clearMemorySegments(List<MemorySegment> segments) {
        // Use batch operations with reusable buffer for better performance
        clearSegments(segments);
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
