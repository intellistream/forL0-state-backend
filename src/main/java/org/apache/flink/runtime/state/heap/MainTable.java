package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    private static final byte NULL_BUCKET_ID = 0;

    private final MemoryManagerAllocator allocator;
    private int bucketCount;
    private List<MemorySegment> memorySegments;
    
    // Track all allocations separately for proper release
    private final List<List<MemorySegment>> allAllocations = new ArrayList<>();
    
    private int[] extensionBucketCounts;
    private int[] extensionBucketBaseIndices;
    private final double loadFactorThreshold;
    private volatile boolean needsResize = false;
    private int totalEntries = 0;
    private int maxExtensionBucketsUsed = 0;
    private int totalBucketCount;  // Cache for total bucket count (main + extension areas)

    private MemorySegment lastFoundSegment = null;
    private int lastFountSlotOffset = -1;

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2) {
        this(allocator, bucketCountPow2, DEFAULT_LOAD_FACTOR_THRESHOLD);
    }

    public MainTable(MemoryManagerAllocator allocator, int bucketCountPow2, double loadFactorThreshold) {
        this.allocator = allocator;
        this.loadFactorThreshold = loadFactorThreshold;
        this.bucketCount = 1 << bucketCountPow2;
        this.totalBucketCount = this.bucketCount;  // Initialize with main buckets only
        
        try {
            // 只分配主桶内存
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            List<MemorySegment> initialAllocation = allocator.allocate((int) totalSize);
            allAllocations.add(initialAllocation);  // 跟踪原始分配
            
            // memorySegments是一个独立的List用于索引访问
            this.memorySegments = new ArrayList<>(initialAllocation);
            clearAllSlots();
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
        
        // 初始化管理数组
        this.extensionBucketBaseIndices = new int[bucketCount];  // 默认0表示未分配
        this.extensionBucketCounts = new int[bucketCount];       // 默认0
    }

    public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return searchBucketTree(bucketIndex, bucketIndex, tag, kb, klen, nb, nlen, arena, segment, bucketOffset);
    }

    /**
     * 插入或更新条目
     * @return 0 for new entry, positive for existing entry address, -1 for full (needs resize)
     */
    public long put(int keyHash, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long result = putInBucketTree(bucketIndex, bucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);

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
        long removed = removeFromBucketTree(bucketIndex, bucketIndex, tag, kb, klen, nb, nlen, arena, segment, bucketOffset);
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
     * 递归搜索桶树
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private long searchBucketTree(int bucketIndex, int mainBucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena,
                                  MemorySegment segment, int bucketOffset) {
        // 先搜索当前桶的槽位
        long result = searchBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, arena);
        if (result != 0) return result;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            return searchBucketTree(extensionBucketIndex, mainBucketIndex, tag, kb, klen, nb, nlen, arena, extensionSegment, extensionBucketOffset);
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
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     * @return 0 for new entry, positive for updated entry address, -1 for full (needs resize)
     */
    private long putInBucketTree(int bucketIndex, int mainBucketIndex, short tag, long entryAddress, byte[] kb, int klen, byte[] nb,
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
            extId = allocateExtensionBucket(mainBucketIndex);
            if (extId == NULL_BUCKET_ID) return -1;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, extId);
        }

        // 递归插入到扩展桶
        int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, extId);
        return putInBucketTree(extensionBucketIndex, mainBucketIndex, tag, entryAddress, kb, klen, nb, nlen, arena);
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
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private long removeFromBucketTree(int bucketIndex, int mainBucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryArena arena,
                                      MemorySegment segment, int bucketOffset) {
        // 先尝试从当前桶删除
        long removed = removeFromBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, arena);
        if (removed != 0) return removed;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            removed = removeFromBucketTree(extensionBucketIndex, mainBucketIndex, tag, kb, klen, nb, nlen, arena, extensionSegment, extensionBucketOffset);
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
            visitBucketTree(bucketIndex, bucketIndex, visitor);
        }
    }

    /**
     * 统一的桶树遍历方法，递归遍历桶及其所有扩展桶
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private void visitBucketTree(int bucketIndex, int mainBucketIndex, EntryVisitor visitor) {
        // 访问当前桶的所有槽位
        visitBucketSlots(bucketIndex, visitor);

        // 检查该主桶是否有扩展区域
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            return;  // 未分配扩展区域
        }
        
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int i = 0; i < EXTENSION_POINTERS; i++) {
            byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
            if (offset != NULL_BUCKET_ID) {
                int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
                visitBucketTree(extensionBucketIndex, mainBucketIndex, visitor);
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

    /**
     * 为指定主桶分配扩展桶，返回相对于该主桶扩展区域的偏移量（1-255）
     * @param mainBucketIndex 主桶索引（必须 < bucketCount）
     * @return 偏移量（1-255），或 NULL_BUCKET_ID(0) 表示失败
     */
    private byte allocateExtensionBucket(int mainBucketIndex) {
        // 检查是否达到单个主桶的扩展上限
        if (extensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return NULL_BUCKET_ID;
        }
        
        // 首次扩展：分配255个桶的连续空间
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            try {
                long extensionAreaSize = (long) MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET * BUCKET_SIZE;
                List<MemorySegment> newSegments = allocator.allocate((int) extensionAreaSize);
                
                // 记录扩展区域的全局起始索引
                extensionBucketBaseIndices[mainBucketIndex] = totalBucketCount;
                totalBucketCount += MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;  // Update cached total
                
                // 追加到memorySegments
                memorySegments.addAll(newSegments);
                clearSegments(newSegments);
                
                // 跟踪这次分配
                allAllocations.add(newSegments);
            } catch (Exception e) {
                return NULL_BUCKET_ID;  // 内存分配失败
            }
        }
        
        // 分配下一个扩展桶（偏移量从1开始）
        byte offset = (byte) (++extensionBucketCounts[mainBucketIndex]);
        
        // 更新统计
        if (extensionBucketCounts[mainBucketIndex] > maxExtensionBucketsUsed) {
            maxExtensionBucketsUsed = extensionBucketCounts[mainBucketIndex];
        }
        
        return offset;
    }

    /**
     * 计算新表的总桶数（resize专用）
     */
    private int calculateTotalBucketCount(int newBucketCount, int[] newExtensionBucketBaseIndices) {
        int total = newBucketCount;
        for (int i = 0; i < newBucketCount; i++) {
            if (newExtensionBucketBaseIndices[i] > 0) {
                total += MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            }
        }
        return total;
    }

    /**
     * 将主桶索引 + 偏移量转换为全局桶索引
     * @param mainBucketIndex 主桶索引
     * @param offset 扩展桶偏移量（1-255）
     * @return 全局桶索引
     */
    private int getExtensionBucketGlobalIndex(int mainBucketIndex, byte offset) {
        int baseIndex = extensionBucketBaseIndices[mainBucketIndex];
        if (baseIndex == 0) {
            throw new IllegalStateException("Extension area not allocated for main bucket " + mainBucketIndex);
        }
        return baseIndex + (offset & 0xFF) - 1;  // offset从1开始，数组索引从0开始
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
        
        // 只分配新的主桶内存
        long newTotalSize = (long) newBucketCount * BUCKET_SIZE;

        List<MemorySegment> newMainBucketsAllocation;
        try {
            newMainBucketsAllocation = allocator.allocate((int) newTotalSize);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }

        // 新表的管理数组和分配跟踪
        int[] newExtensionBucketBaseIndices = new int[newBucketCount];
        int[] newExtensionBucketCounts = new int[newBucketCount];
        List<List<MemorySegment>> newAllAllocations = new ArrayList<>();
        newAllAllocations.add(newMainBucketsAllocation);  // 跟踪主桶分配
        
        // 创建独立的索引用List
        List<MemorySegment> newMemorySegments = new ArrayList<>(newMainBucketsAllocation);
        clearMemorySegments(newMemorySegments);

        // 直接迁移条目，无需中间集合
        migrateAllEntriesToNewTable(newMemorySegments, newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations, entryArena);

        // 释放所有旧表的分配（包括主桶和动态扩展桶）
        for (List<MemorySegment> allocation : allAllocations) {
            allocator.release(allocation);
        }
        
        // 切换到新表
        this.allAllocations.clear();
        this.allAllocations.addAll(newAllAllocations);
        this.memorySegments = newMemorySegments;
        this.extensionBucketBaseIndices = newExtensionBucketBaseIndices;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        this.totalBucketCount = calculateTotalBucketCount(newBucketCount, newExtensionBucketBaseIndices);  // Recalculate total
        needsResize = false;
        maxExtensionBucketsUsed = Arrays.stream(extensionBucketCounts).max().orElse(0);
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
                                             int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                             List<List<MemorySegment>> newAllAllocations, EntryArena entryArena) {
        // 遍历所有基桶，每个基桶会递归遍历其扩展子树
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            migrateBucketTree(bucketIndex, bucketIndex, newMemorySegments, newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations, entryArena);
        }
    }

    /**
     * 递归迁移一个桶及其所有扩展桶的条目
     * 体现桶的树形结构：基桶是根节点，扩展桶是子节点
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private void migrateBucketTree(int bucketIndex, int mainBucketIndex, List<MemorySegment> newMemorySegments, int newBucketCount,
                                   int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                   List<List<MemorySegment>> newAllAllocations, EntryArena entryArena) {
        // 迁移当前桶的所有槽位数据
        migrateBucketSlots(bucketIndex, newMemorySegments, newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations, entryArena);

        // 如果是基桶，递归迁移所有扩展桶的数据
        if (bucketIndex < bucketCount) {
            // 检查是否有扩展区域
            if (extensionBucketBaseIndices[bucketIndex] == 0) {
                return;  // 该主桶没有扩展桶
            }
            
            MemorySegment segment = getSegmentForBucket(bucketIndex);
            int bucketOffset = getBucketOffsetInSegment(bucketIndex);

            // 遍历4个扩展桶指针
            for (int i = 0; i < EXTENSION_POINTERS; i++) {
                byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + i);
                if (offset != NULL_BUCKET_ID) {
                    // 递归迁移扩展桶及其可能的子扩展桶
                    int extensionBucketIndex = getExtensionBucketGlobalIndex(bucketIndex, offset);
                    migrateBucketTree(extensionBucketIndex, bucketIndex, newMemorySegments, newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations, entryArena);
                }
            }
        }
    }
    private void migrateBucketSlots(int bucketIndex, List<MemorySegment> newMemorySegments, int newBucketCount,
                                    int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                    List<List<MemorySegment>> newAllAllocations, EntryArena entryArena) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++) {
            int slotOffset = bucketOffset + slot * SLOT_SIZE;
            long entryAddress = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (entryAddress == 0) continue;

            short tag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);

            // 直接从EntryArena读取存储的hash值，无需重新计算
            int fullHash = entryArena.getHash(entryAddress);
            int newBucketIndex = fullHash & (newBucketCount - 1);

            // 复用现有的递归插入逻辑，支持多级扩展桶
            putInNewTable(newMemorySegments, newBucketCount, newBucketIndex, tag, entryAddress, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations);
        }
    }

    private void putInNewTable(List<MemorySegment> newMemorySegments, int newBucketCount, int bucketIndex,
                              short tag, long entryAddress, int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                              List<List<MemorySegment>> newAllAllocations) {
        // 确定主桶索引（首次调用时 bucketIndex < newBucketCount 必然成立）
        int mainBucketIndex = bucketIndex < newBucketCount ? bucketIndex : getMainBucketIndexForNewTable(bucketIndex, newBucketCount, newExtensionBucketBaseIndices);
        putInNewTableWithMainBucket(newMemorySegments, newBucketCount, bucketIndex, mainBucketIndex, tag, entryAddress, 
                                   newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations);
    }

    private void putInNewTableWithMainBucket(List<MemorySegment> newMemorySegments, int newBucketCount, int bucketIndex, int mainBucketIndex,
                              short tag, long entryAddress, int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                              List<List<MemorySegment>> newAllAllocations) {
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

        // 当前桶满，根据tag确定扩展桶
        int extensionIndex = tag & 0x3;
        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (offset == NULL_BUCKET_ID) {
            // 为新表的主桶分配扩展桶
            offset = allocateExtensionBucketForNewTable(mainBucketIndex, newMemorySegments, newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations);
            if (offset == NULL_BUCKET_ID) return; // 无法分配，跳过
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, offset);
        }

        // 递归插入到扩展桶
        int extensionBucketIndex = newExtensionBucketBaseIndices[mainBucketIndex] + (offset & 0xFF) - 1;
        putInNewTableWithMainBucket(newMemorySegments, newBucketCount, extensionBucketIndex, mainBucketIndex, tag, entryAddress, 
                                   newExtensionBucketBaseIndices, newExtensionBucketCounts, newAllAllocations);
    }

    /**
     * 为新表的主桶分配扩展桶（resize专用）
     */
    private byte allocateExtensionBucketForNewTable(int mainBucketIndex, List<MemorySegment> newMemorySegments, int newBucketCount,
                                                    int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                                    List<List<MemorySegment>> newAllAllocations) {
        // 检查是否达到单个主桶的扩展上限
        if (newExtensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return NULL_BUCKET_ID;
        }
        
        // 首次扩展：分配255个桶的连续空间
        if (newExtensionBucketBaseIndices[mainBucketIndex] == 0) {
            try {
                long extensionAreaSize = (long) MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET * BUCKET_SIZE;
                List<MemorySegment> extensionSegments = allocator.allocate((int) extensionAreaSize);
                
                // 计算当前新表的总桶数
                int currentTotalBuckets = newBucketCount;
                for (int i = 0; i < newBucketCount; i++) {
                    if (newExtensionBucketBaseIndices[i] > 0) {
                        currentTotalBuckets += MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
                    }
                }
                
                newExtensionBucketBaseIndices[mainBucketIndex] = currentTotalBuckets;
                newMemorySegments.addAll(extensionSegments);
                clearSegments(extensionSegments);
                
                // 跟踪这次新表的扩展桶分配
                newAllAllocations.add(extensionSegments);
            } catch (Exception e) {
                return NULL_BUCKET_ID;
            }
        }
        
        // 分配下一个扩展桶
        byte offset = (byte) (++newExtensionBucketCounts[mainBucketIndex]);
        return offset;
    }
    /**
     * 从新表的全局桶索引反推主桶索引（resize专用）
     */
    private int getMainBucketIndexForNewTable(int globalBucketIndex, int newBucketCount, int[] newExtensionBucketBaseIndices) {
        if (globalBucketIndex < newBucketCount) {
            return globalBucketIndex;
        }
        
        for (int i = 0; i < newBucketCount; i++) {
            int baseIndex = newExtensionBucketBaseIndices[i];
            if (baseIndex > 0 && globalBucketIndex >= baseIndex 
                && globalBucketIndex < baseIndex + MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
                return i;
            }
        }
        
        throw new IllegalStateException("Invalid global bucket index in new table: " + globalBucketIndex);
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

    private int getAllocatedExtensionBuckets() {
        int count = 0;
        for (int i = 0; i < bucketCount; i++) {
            if (extensionBucketBaseIndices[i] > 0) {
                count += extensionBucketCounts[i];  // 累加实际使用的扩展桶数
            }
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
        if (allocator != null) {
            // 释放所有跟踪的内存分配（主桶 + 所有动态扩展桶）
            for (List<MemorySegment> allocation : allAllocations) {
                allocator.release(allocation);
            }
            allAllocations.clear();
        }
    }
}
