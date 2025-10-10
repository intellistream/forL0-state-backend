package org.apache.flink.runtime.state.heap.legacy;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import java.util.List;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

/**
 * **Deprecated** Legacy implementation using sun.misc.Unsafe.
 * This is intentional for the Cavast legacy backend.
 */
@SuppressWarnings("restriction")
public class CavastBucketTable implements AutoCloseable {

    private MemorySegment slab; // 64B * bucketCap
    private final int bucketCapMask; // 2^n - 1
    private final MemoryManagerAllocator alloc;
    private final int rootBucketCount;
    private int totalBuckets;

    /** 64B Bucket */
    private static final int SLOT_TAG_OFF = 0;
    private static final int SLOT_PTR_OFF = 2;
    private static final int SLOT_STEP = 10;
    private static final int CHILD_BASE = 60;
    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    private int nextFree = 0;

    CavastBucketTable(int initCapPow2, MemoryManagerAllocator alloc) {
        this.alloc = alloc;
        int cap = 1 << initCapPow2;
        List<MemorySegment> pages = null;
        try {
            pages = alloc.allocate(cap * 64);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException("Failed to allocate memory for ForL0BucketTable", e);
        }
        assert pages.size() == 1 : "ForL0BucketTable expects a contiguous slab; increase segment-size to accommodate";
        this.slab = pages.get(0);
        // zero‑fill the root buckets so that child‑index bytes start at 0
        UNSAFE.setMemory(this.slab.getAddress(), cap * 64L, (byte) 0);
        this.bucketCapMask = cap - 1;
        this.rootBucketCount = cap;
        this.totalBuckets = rootBucketCount;
    }

    private long bucketAddr(int idx) {
        assert idx < totalBuckets
                : "bucket index out of slab range: " + idx + " >= " + totalBuckets;
        return slab.getAddress() + ((long) idx << 6); // *64
    }

    /**
     * Return the pointer or 0
     */
    long lookup(byte[] key, byte[] ns, int hash, short tag) {
        int idx = hash & bucketCapMask;
        long bucket = bucketAddr(idx);
        for (;;) {
            long addr = seekInBucket(bucket, tag, key, ns);
            if (addr != 0)
                return addr; // hit
            int childIdx = UNSAFE.getByte(bucket + CHILD_BASE + (tag & 0x3));
            if (childIdx == 0)
                return 0; // no child, miss
            bucket = bucketAddr(childIdx & 0xFF);
            tag >>>= 2;
        }
    }

    private long seekInBucket(long bucket, short tag, byte[] key, byte[] ns) {
        for(int s = 0; s < 6; s++) {
            int off = SLOT_STEP * s;
            short tg = UNSAFE.getShort(bucket + off + SLOT_TAG_OFF);
            long ptr = UNSAFE.getLong(bucket + off + SLOT_PTR_OFF);
            if (ptr == 0)
                return 0;
            if(tg == tag && CavastEntryAccess.equalKN(ptr, key, ns))
                return ptr;
        }
        return 0;
    }

    /**
     * Insert a pointer, expand a child bucket if full
     */
    void insert(int hash, short tag, long ptr) {
        int idx = hash & bucketCapMask;
        insertIntoBucket(bucketAddr(idx), tag, ptr, hash, 0);
    }

    private void insertIntoBucket(long bucket, short tag, long ptr, int hash, int depth) {
        // try to insert into an empty slot in this bucket
        for(int s = 0; s < 6; s++) {
            int off = SLOT_STEP * s;
            if (UNSAFE.getLong(bucket + off + SLOT_PTR_OFF) == 0) {
                UNSAFE.putShort(bucket + off + SLOT_TAG_OFF, tag);
                UNSAFE.putLong(bucket + off + SLOT_PTR_OFF, ptr);
                return;
            }
        }
        // bucket is full, expand locally
        int childPos = tag & 0x3;
        long childIdxAddr = bucket + CHILD_BASE + childPos;
        int childIdx = UNSAFE.getByte(childIdxAddr) & 0xFF;
        if (childIdx == 0) {
            childIdx = allocateNewBucket();
            UNSAFE.putByte(childIdxAddr, (byte) childIdx);
        }
        insertIntoBucket(bucketAddr(childIdx), (short)(tag >>> 2), ptr, hash, depth + 1);
    }

    private int allocateNewBucket() {
        // 若需要的索引超出现有 slab, 先扩容 (×2)
        if (rootBucketCount + nextFree >= totalBuckets) {
            growSlab();
        }
        int idx = rootBucketCount + (nextFree++);
        long p = bucketAddr(idx);
        UNSAFE.setMemory(p, 64, (byte) 0);
        return idx;
    }

    /** slab 翻倍并拷贝原内容 */
    private void growSlab() {
        int newBuckets = totalBuckets << 1;
        int bytesNeeded = newBuckets * 64;
        List<MemorySegment> pages = null;
        try {
            pages = alloc.allocate(bytesNeeded);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException("Failed to allocate memory for ForL0BucketTable", e);
        }
        assert pages.size() == 1 : "ForL0BucketTable requires a single contiguous MemorySegment; configure larger segment-size";
        MemorySegment bigger = pages.get(0);

        // copy old slab content
        UnsafeUtils.unsafe().copyMemory(
                null, slab.getAddress(),
                null, bigger.getAddress(),
                (long) totalBuckets * 64);

        slab.free();            // return old slab to MemoryManager
        slab = bigger;
        totalBuckets = newBuckets;
    }

    @Override
    public void close() {
        if (slab != null) {
            slab.free();
            slab = null;
        }
    }
}
