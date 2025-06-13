package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.space.MemorySlice;
import org.apache.flink.runtime.state.heap.space.SimpleUnsafeMemoryAllocator;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

public class OffHeapBucketTable {

    private final MemorySlice slab; // 64B * bucketCap
    private final int bucketCapMask; // 2^n - 1
    private final SimpleUnsafeMemoryAllocator alloc;

    /** 64B Bucket */
    private static final int SLOT_TAG_OFF = 0;
    private static final int SLOT_PTR_OFF = 2;
    private static final int SLOT_STEP = 10;
    private static final int CHILD_BASE = 60;
    private static final Unsafe UNSAFE = UnsafeUtils.unsafe();

    private int nextFree = 0;

    OffHeapBucketTable(int initCapPow2, SimpleUnsafeMemoryAllocator alloc) {
        this.alloc = alloc;
        int cap = 1 << initCapPow2;
        this.slab = alloc.allocate(cap * 64);
        this.bucketCapMask = cap - 1;
    }

    private long bucketAddr(int idx) {
        return slab.address() + ((long) idx << 6); // *64
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
            if(tg == tag && OffHeapEntryAccess.equalKN(ptr, key, ns))
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
        int idx = 64 + (nextFree++);
        long p = bucketAddr(idx);
        UNSAFE.setMemory(p, 64, (byte) 0);
        return idx;
    }

}



