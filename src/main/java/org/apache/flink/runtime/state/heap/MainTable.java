package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.entrystore.EntryStore;
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
    
    // Extension pool configuration
    private static final int EXTENSION_AREA_SIZE = MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET * BUCKET_SIZE;  // 16320 bytes per area
    private static final int POOL_GROW_AREAS = 32;  // Allocate 32 extension areas at a time

    private final MemoryManagerAllocator allocator;
    private int bucketCount;
    
    // Segment storage: array for O(1) access
    private MemorySegment[] memorySegments;
    private int memorySegmentCount;
    private int segmentSize;  // Cached segment size to avoid repeated .size() calls
    
    // Track all allocations separately for proper release
    private final List<List<MemorySegment>> allAllocations = new ArrayList<>();
    
    // Extension bucket pool management
    private int extensionPoolCapacity;  // Number of extension areas available in pool
    private int extensionPoolUsed;      // Number of extension areas allocated to main buckets
    
    private int[] extensionBucketCounts;
    private int[] extensionBucketBaseIndices;
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
        this.loadFactorThreshold = loadFactorThreshold;
        this.bucketCount = 1 << bucketCountPow2;
        
        try {
            // 只分配主桶内存
            long totalSize = (long) bucketCount * BUCKET_SIZE;
            List<MemorySegment> initialAllocation = allocator.allocate((int) totalSize);
            allAllocations.add(initialAllocation);  // 跟踪原始分配
            
            // 使用数组存储，O(1) 访问
            this.memorySegments = initialAllocation.toArray(new MemorySegment[0]);
            this.memorySegmentCount = this.memorySegments.length;
            this.segmentSize = this.memorySegments[0].size();
            
            clearAllSlots();
        } catch (Exception e) {
            throw new RuntimeException("Failed to allocate Main Table memory", e);
        }
        
        // 初始化管理数组
        this.extensionBucketBaseIndices = new int[bucketCount];  // 默认0表示未分配
        this.extensionBucketCounts = new int[bucketCount];       // 默认0
        
        // 初始化扩展池状态
        this.extensionPoolCapacity = 0;
        this.extensionPoolUsed = 0;
    }

    public long get(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        return searchBucketTree(bucketIndex, bucketIndex, tag, kb, klen, nb, nlen, store, segment, bucketOffset);
    }

    /**
     * 插入或更新条目
     * @return 0 for new entry, positive for existing entry address, -1 for full (needs resize)
     */
    public long put(int keyHash, short tag, long entryAddress, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        long result = putInBucketTree(bucketIndex, bucketIndex, tag, entryAddress, kb, klen, nb, nlen, store);

        if (result == 0) {
            totalEntries++;
            checkResizeNeeded();
        } else if (result == -1) {
            needsResize = true;
        }

        return result;
    }

    public long remove(int keyHash, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store) {
        int bucketIndex = keyHash & (bucketCount - 1);
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long removed = removeFromBucketTree(bucketIndex, bucketIndex, tag, kb, klen, nb, nlen, store, segment, bucketOffset);
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
    private long searchBucketTree(int bucketIndex, int mainBucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store,
                                  MemorySegment segment, int bucketOffset) {
        // 先搜索当前桶的槽位
        long result = searchBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, store);
        if (result != 0) return result;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            return searchBucketTree(extensionBucketIndex, mainBucketIndex, tag, kb, klen, nb, nlen, store, extensionSegment, extensionBucketOffset);
        }
        return 0;
    }

    private long searchBucketSlots(MemorySegment segment, int bucketOffset, short tag, byte[] kb, int klen, byte[] nb,
                                   int nlen, EntryStore store) {
        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag == tag && store.matchesKey(ptr, kb, klen, nb, nlen)) {
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
                                 int nlen, EntryStore store) {
        // 先尝试插入当前桶
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);
        long result = putInSlots(segment, bucketOffset, tag, entryAddress, kb, klen, nb, nlen, store);
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
        return putInBucketTree(extensionBucketIndex, mainBucketIndex, tag, entryAddress, kb, klen, nb, nlen, store);
    }

    /**
     * Performs a put operation on bucket slots with unified logic.
     * @return 0 for new entry, positive for existing entry address, -1 for full bucket
     */
    private long putInSlots(MemorySegment segment, int bucketOffset, short tag, long entryAddress,
                            byte[] kb, int klen, byte[] nb, int nlen, EntryStore store) {
        int empty = -1;
        int emptyOffset = 0;

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);

            if (ptr == 0) {
                if (empty == -1) {
                    empty = slot;
                    emptyOffset = slotOffset;
                }
                continue;
            }

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag == tag && store.matchesKey(ptr, kb, klen, nb, nlen)) {
                lastFoundSegment = segment;
                lastFountSlotOffset = slotOffset;
                if (entryAddress > 0) segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return ptr;
            }
        }

        if (empty != -1) {
            lastFoundSegment = segment;
            lastFountSlotOffset = emptyOffset;
            segment.putShort(emptyOffset + SLOT_TAG_OFFSET, tag);
            if (entryAddress > 0) segment.putLong(emptyOffset + SLOT_POINTER_OFFSET, entryAddress);
            return 0;
        }
        return -1;
    }

    /**
     * 递归从桶树中删除条目，不释放空扩展桶
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private long removeFromBucketTree(int bucketIndex, int mainBucketIndex, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store,
                                      MemorySegment segment, int bucketOffset) {
        // 先尝试从当前桶删除
        long removed = removeFromBucketSlots(segment, bucketOffset, tag, kb, klen, nb, nlen, store);
        if (removed != 0) return removed;

        // 根据tag确定扩展桶指针索引
        int extensionIndex = tag & 0x3;  // tag的低2位决定使用哪个扩展指针

        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);
        if (offset != NULL_BUCKET_ID) {
            int extensionBucketIndex = getExtensionBucketGlobalIndex(mainBucketIndex, offset);
            MemorySegment extensionSegment = getSegmentForBucket(extensionBucketIndex);
            int extensionBucketOffset = getBucketOffsetInSegment(extensionBucketIndex);
            removed = removeFromBucketTree(extensionBucketIndex, mainBucketIndex, tag, kb, klen, nb, nlen, store, extensionSegment, extensionBucketOffset);
        }
        return removed;
    }

    private long removeFromBucketSlots(MemorySegment segment, int bucketOffset, short tag, byte[] kb, int klen, byte[] nb, int nlen, EntryStore store) {
        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) continue;

            short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            if (slotTag == tag && store.matchesKey(ptr, kb, klen, nb, nlen)) {
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

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long slotPointer = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (slotPointer != 0) {
                short slotTag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
                visitor.visit(slotPointer, bucketIndex, slotTag);
            }
        }
    }

    // --- Extension bucket management ---

    /**
     * 批量增长扩展桶内存池。
     * 这是分配扩展区域内存的唯一入口，将多次小分配合并为批量分配。
     * @param areasToAdd 要添加的扩展区域数量
     */
    private void growExtensionPool(int areasToAdd) {
        long totalSize = (long) areasToAdd * EXTENSION_AREA_SIZE;
        try {
            List<MemorySegment> newSegments = allocator.allocate((int) totalSize);
            clearSegmentList(newSegments);
            
            // 扩展数组
            int oldLen = memorySegmentCount;
            int newLen = oldLen + newSegments.size();
            MemorySegment[] newArray = Arrays.copyOf(memorySegments, newLen);
            for (int i = 0; i < newSegments.size(); i++) {
                newArray[oldLen + i] = newSegments.get(i);
            }
            memorySegments = newArray;
            memorySegmentCount = newLen;
            
            allAllocations.add(newSegments);
            extensionPoolCapacity += areasToAdd;
        } catch (Exception e) {
            throw new RuntimeException("Failed to grow extension pool", e);
        }
    }

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
        
        // 首次扩展该主桶：从池中分配一个扩展区域
        if (extensionBucketBaseIndices[mainBucketIndex] == 0) {
            // 池容量不足时批量增长
            if (extensionPoolUsed >= extensionPoolCapacity) {
                growExtensionPool(POOL_GROW_AREAS);
            }
            // 从池中分配（O(1) 操作，无系统调用）
            extensionBucketBaseIndices[mainBucketIndex] = bucketCount + extensionPoolUsed * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            extensionPoolUsed++;
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

    public void tryResize(EntryStore entryStore) {
        if (!needsResize) return;
        resize(entryStore);
    }

    public void resize(EntryStore entryStore) {
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
        
        // 新表的扩展池状态
        int[] newPoolState = new int[2];  // [0]=capacity, [1]=used
        
        // 使用数组存储（执行迁移时会动态扩展）
        MemorySegment[] newMemorySegmentsArray = newMainBucketsAllocation.toArray(new MemorySegment[0]);
        int newSegmentSize = newMemorySegmentsArray[0].size();
        int[] newSegmentCount = {newMemorySegmentsArray.length};  // 使用数组以便在内部方法中修改
        MemorySegment[][] newSegmentsHolder = {newMemorySegmentsArray};  // 使用持有者以便扩展
        
        clearSegmentArray(newMemorySegmentsArray, newSegmentCount[0]);

        // 直接迁移条目，无需中间集合
        migrateAllEntriesToNewTable(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                    newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                    newAllAllocations, newPoolState, entryStore);

        // 释放所有旧表的分配（包括主桶和动态扩展桶）
        for (List<MemorySegment> allocation : allAllocations) {
            allocator.release(allocation);
        }
        
        // 切换到新表
        this.allAllocations.clear();
        this.allAllocations.addAll(newAllAllocations);
        this.memorySegments = newSegmentsHolder[0];
        this.memorySegmentCount = newSegmentCount[0];
        this.segmentSize = newSegmentSize;
        this.extensionBucketBaseIndices = newExtensionBucketBaseIndices;
        this.extensionBucketCounts = newExtensionBucketCounts;
        this.bucketCount = newBucketCount;
        this.extensionPoolCapacity = newPoolState[0];
        this.extensionPoolUsed = newPoolState[1];
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
    private void migrateAllEntriesToNewTable(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount, 
                                             int newSegmentSize, int newBucketCount,
                                             int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                             List<List<MemorySegment>> newAllAllocations, int[] newPoolState, 
                                             EntryStore entryStore) {
        // 遍历所有基桶，每个基桶会递归遍历其扩展子树
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            migrateBucketTree(bucketIndex, bucketIndex, newSegmentsHolder, newSegmentCount, newSegmentSize, 
                              newBucketCount, newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                              newAllAllocations, newPoolState, entryStore);
        }
    }

    /**
     * 递归迁移一个桶及其所有扩展桶的条目
     * 体现桶的树形结构：基桶是根节点，扩展桶是子节点
     * @param bucketIndex 当前桶的全局索引
     * @param mainBucketIndex 当前桶所属的主桶索引
     */
    private void migrateBucketTree(int bucketIndex, int mainBucketIndex, 
                                   MemorySegment[][] newSegmentsHolder, int[] newSegmentCount, int newSegmentSize,
                                   int newBucketCount,
                                   int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                   List<List<MemorySegment>> newAllAllocations, int[] newPoolState, 
                                   EntryStore entryStore) {
        // 迁移当前桶的所有槽位数据
        migrateBucketSlots(bucketIndex, newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                           newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                           newAllAllocations, newPoolState, entryStore);

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
                    migrateBucketTree(extensionBucketIndex, bucketIndex, newSegmentsHolder, newSegmentCount, 
                                      newSegmentSize, newBucketCount, newExtensionBucketBaseIndices, 
                                      newExtensionBucketCounts, newAllAllocations, newPoolState, entryStore);
                }
            }
        }
    }
    
    private void migrateBucketSlots(int bucketIndex, MemorySegment[][] newSegmentsHolder, int[] newSegmentCount,
                                    int newSegmentSize, int newBucketCount,
                                    int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                    List<List<MemorySegment>> newAllAllocations, int[] newPoolState, 
                                    EntryStore entryStore) {
        MemorySegment segment = getSegmentForBucket(bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex);

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long entryAddress = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (entryAddress == 0) continue;

            short tag = segment.getShort(slotOffset + SLOT_TAG_OFFSET);
            int fullHash = entryStore.getHash(entryAddress);
            int newBucketIndex = fullHash & (newBucketCount - 1);

            putInNewTable(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                          newBucketIndex, tag, entryAddress, 
                          newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                          newAllAllocations, newPoolState);
        }
    }

    private void putInNewTable(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount, 
                               int newSegmentSize, int newBucketCount, int bucketIndex,
                               short tag, long entryAddress, 
                               int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                               List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        // 确定主桶索引（首次调用时 bucketIndex < newBucketCount 必然成立）
        int mainBucketIndex = bucketIndex < newBucketCount ? bucketIndex : 
                              getMainBucketIndexForNewTable(bucketIndex, newBucketCount, newExtensionBucketBaseIndices);
        putInNewTableWithMainBucket(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                    bucketIndex, mainBucketIndex, tag, entryAddress, 
                                    newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                    newAllAllocations, newPoolState);
    }

    private void putInNewTableWithMainBucket(MemorySegment[][] newSegmentsHolder, int[] newSegmentCount,
                                             int newSegmentSize, int newBucketCount, 
                                             int bucketIndex, int mainBucketIndex,
                                             short tag, long entryAddress, 
                                             int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                             List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        MemorySegment segment = getSegmentForBucket(newSegmentsHolder[0], newSegmentCount[0], newSegmentSize, bucketIndex);
        int bucketOffset = getBucketOffsetInSegment(bucketIndex, newSegmentSize);

        int slotOffset = bucketOffset;
        for (int slot = 0; slot < SLOTS_PER_BUCKET; slot++, slotOffset += SLOT_SIZE) {
            long ptr = segment.getLong(slotOffset + SLOT_POINTER_OFFSET);
            if (ptr == 0) {
                segment.putShort(slotOffset + SLOT_TAG_OFFSET, tag);
                segment.putLong(slotOffset + SLOT_POINTER_OFFSET, entryAddress);
                return;
            }
        }

        // 当前桶满，根据tag确定扩展桶
        int extensionIndex = tag & 0x3;
        byte offset = segment.get(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex);

        if (offset == NULL_BUCKET_ID) {
            offset = allocateExtensionBucketForNewTable(mainBucketIndex, newSegmentsHolder, newSegmentCount, 
                                                        newSegmentSize, newBucketCount, 
                                                        newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                                        newAllAllocations, newPoolState);
            if (offset == NULL_BUCKET_ID) return;
            segment.put(bucketOffset + EXTENSION_POINTERS_OFFSET + extensionIndex, offset);
        }

        int extensionBucketIndex = newExtensionBucketBaseIndices[mainBucketIndex] + (offset & 0xFF) - 1;
        putInNewTableWithMainBucket(newSegmentsHolder, newSegmentCount, newSegmentSize, newBucketCount, 
                                    extensionBucketIndex, mainBucketIndex, tag, entryAddress, 
                                    newExtensionBucketBaseIndices, newExtensionBucketCounts, 
                                    newAllAllocations, newPoolState);
    }

    /**
     * 为新表的主桶分配扩展桶（resize专用，使用池化逻辑）
     */
    private byte allocateExtensionBucketForNewTable(int mainBucketIndex, 
                                                    MemorySegment[][] newSegmentsHolder, int[] newSegmentCount,
                                                    int newSegmentSize, int newBucketCount,
                                                    int[] newExtensionBucketBaseIndices, int[] newExtensionBucketCounts,
                                                    List<List<MemorySegment>> newAllAllocations, int[] newPoolState) {
        // 检查是否达到单个主桶的扩展上限
        if (newExtensionBucketCounts[mainBucketIndex] >= MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET) {
            return NULL_BUCKET_ID;
        }
        
        // 首次扩展该主桶：从池中分配一个扩展区域
        if (newExtensionBucketBaseIndices[mainBucketIndex] == 0) {
            // 池容量不足时批量增长
            if (newPoolState[1] >= newPoolState[0]) {
                // 批量分配扩展区域
                long totalSize = (long) POOL_GROW_AREAS * EXTENSION_AREA_SIZE;
                try {
                    List<MemorySegment> newSegments = allocator.allocate((int) totalSize);
                    clearSegmentList(newSegments);
                    
                    // 扩展数组
                    int oldLen = newSegmentCount[0];
                    int addLen = newSegments.size();
                    MemorySegment[] expanded = Arrays.copyOf(newSegmentsHolder[0], oldLen + addLen);
                    for (int i = 0; i < addLen; i++) {
                        expanded[oldLen + i] = newSegments.get(i);
                    }
                    newSegmentsHolder[0] = expanded;
                    newSegmentCount[0] = oldLen + addLen;
                    
                    newAllAllocations.add(newSegments);
                    newPoolState[0] += POOL_GROW_AREAS;
                } catch (Exception e) {
                    return NULL_BUCKET_ID;
                }
            }
            // 从池中分配（O(1) 操作）
            newExtensionBucketBaseIndices[mainBucketIndex] = newBucketCount + newPoolState[1] * MAX_EXTENSION_BUCKETS_PER_MAIN_BUCKET;
            newPoolState[1]++;
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

    /**
     * 获取指定桶所在的 MemorySegment。
     */
    private MemorySegment getSegmentForBucket(int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segmentSize;
        return memorySegments[Math.min(segmentIndex, memorySegmentCount - 1)];
    }

    /**
     * resize 专用：从新表的 segment 列表获取 segment。
     */
    private MemorySegment getSegmentForBucket(MemorySegment[] segments, int segmentCount, int segSize, int bucketIndex) {
        int segmentIndex = (bucketIndex * BUCKET_SIZE) / segSize;
        return segments[Math.min(segmentIndex, segmentCount - 1)];
    }

    /**
     * 获取桶在 segment 内的偏移量（使用缓存的 segmentSize）。
     */
    private int getBucketOffsetInSegment(int bucketIndex) {
        return ((bucketIndex * BUCKET_SIZE) % segmentSize);
    }

    /**
     * resize 专用：使用指定的 segmentSize 计算偏移量。
     */
    private int getBucketOffsetInSegment(int bucketIndex, int segSize) {
        return ((bucketIndex * BUCKET_SIZE) % segSize);
    }

    private void clearAllSlots() {
        // Use batch operations with reusable buffer for better performance
        clearSegmentArray(memorySegments, memorySegmentCount);
    }

    /**
     * 清空 segment 数组中的所有内容。
     */
    private void clearSegmentArray(MemorySegment[] segments, int count) {
        byte[] zeroArray = new byte[1024]; // Reusable zero buffer
        for (int i = 0; i < count; i++) {
            MemorySegment segment = segments[i];
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

    /**
     * 清空 segment List 中的所有内容（用于 resize 等场景）。
     */
    private void clearSegmentList(List<MemorySegment> segments) {
        byte[] zeroArray = new byte[1024]; // Reusable zero buffer
        for (MemorySegment segment : segments) {
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
