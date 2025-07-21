package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;
import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

import java.util.List;

public final class OffHeapLevelHashIndex implements AutoCloseable {

    private static final Unsafe U = UnsafeUtils.unsafe();

    private final MemoryManagerAllocator allocator;
    private final long pageSize;

    // table metadata
    private long topBase;         // address of first top‑level bucket
    private long bottomBase;      // address of first bottom bucket
    private int  topCapacity;     // number of top buckets (power‑of‑two)

    private int size;             // number of stored items
    // own all memory segments allocated for this index
    private final java.util.List<MemorySegment> ownedSegs = new java.util.ArrayList<>();

    // ---------------------------------------------------------------------
    /** Creates a Level‑Hash index with given power‑of‑two top capacity (e.g. 16 ⇒ 65536 buckets). */
    public OffHeapLevelHashIndex(MemoryManagerAllocator allocator,
                                 int initialCapacityPow2) {
        if (initialCapacityPow2 < 4 || initialCapacityPow2 > 28) {
            throw new IllegalArgumentException("capacity pow2 out of range: " + initialCapacityPow2);
        }
        this.allocator = allocator;
        this.pageSize  = allocator.getPageSize();
        try {
            allocateTables(1 << initialCapacityPow2);
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    // ---------------------------------------------------------------------
    /** User‑facing put API – delegates to insert(). */
    public long put(int keyHash, long entryPtr) throws MemoryAllocationException {
        return insert(keyHash, entryPtr);
    }

    /** User‑facing remove – returns previous ptr (>0) or 0 if absent. */
    public long remove(int keyHash) {
        long h1 = HashFunctions.mix64(keyHash);
        long h2 = HashFunctions.mix64(Long.rotateLeft(keyHash, 17));
        int fp  = HashFunctions.tag(h1);

        int idx1 = (int) (h1 & (topCapacity - 1));
        int idx2 = (int) (h2 & (topCapacity - 1));
        int sb1  = (int) (h1 & ((topCapacity >> 1) - 1));
        int sb2  = (int) (h2 & ((topCapacity >> 1) - 1));

        long old = deleteFromBucket(topBase + ((long) idx1 * BucketLayout.BUCKET_SIZE), fp);
        if (old != 0) { size--; return old; }
        old = deleteFromBucket(topBase + ((long) idx2 * BucketLayout.BUCKET_SIZE), fp);
        if (old != 0) { size--; return old; }
        old = deleteFromBottomChain(sb1, fp);
        if (old != 0) { size--; return old; }
        old = deleteFromBottomChain(sb2, fp);
        if (old != 0) { size--; return old; }
        return 0;
    }

    // ---------------------------------------------------------------------
    //  Public CRUD API ----------------------------------------------------------------
    // ---------------------------------------------------------------------

    /**
     * Insert new (tag,ptr) pair.  Return previous ptr (>0) if key already exists, otherwise 0.
     */
    public long insert(int keyHash, long entryPtr) throws MemoryAllocationException {
        long h1 = HashFunctions.mix64(keyHash);
        long h2 = HashFunctions.mix64(Long.rotateLeft(keyHash, 17));
        int fp  = HashFunctions.tag(h1);

        int idx1 = (int) (h1 & (topCapacity - 1));
        int idx2 = (int) (h2 & (topCapacity - 1));
        int sb1  = (int) (h1 & ((topCapacity >> 1) - 1));
        int sb2  = (int) (h2 & ((topCapacity >> 1) - 1));

        // 1) search & maybe update in top
        long prev = searchAndMaybeUpdate(idx1, fp, entryPtr);
        if (prev != 0) { return prev; }
        prev = searchAndMaybeUpdate(idx2, fp, entryPtr);
        if (prev != 0) { return prev; }

        // Resize trigger before insertion
        if (size + 1 > (int)(topCapacity * 0.9)) {
            resize();
        }

        // 2) try insert into emptier top bucket
        if (tryInsertIntoTop(idx1, idx2, fp, entryPtr)) {
            size++; return 0;
        }

        // 3) single relocation attempt
        if (relocateOnce(idx1, fp, entryPtr) || relocateOnce(idx2, fp, entryPtr)) {
            size++; return 0;
        }

        // 4) fallback: insert into bottom buckets
        if (tryInsertIntoBottom(sb1, fp, entryPtr) || tryInsertIntoBottom(sb2, fp, entryPtr)) {
            size++; return 0;
        }

        // 5) need resize
        resize();
        return insert(keyHash, entryPtr); // tail recursion
    }

    /** Lookup – return ptr or 0. */
    public long get(int keyHash) {
        long h1 = HashFunctions.mix64(keyHash);
        long h2 = HashFunctions.mix64(Long.rotateLeft(keyHash, 17));
        int fp  = HashFunctions.tag(h1);

        int idx1 = (int) (h1 & (topCapacity - 1));
        int idx2 = (int) (h2 & (topCapacity - 1));
        int sb1  = (int) (h1 & ((topCapacity >> 1) - 1));
        int sb2  = (int) (h2 & ((topCapacity >> 1) - 1));

        long p = searchBucket(topBase + ((long) idx1 * BucketLayout.BUCKET_SIZE), fp);
        if (p != 0) { return p; }
        p = searchBucket(topBase + ((long) idx2 * BucketLayout.BUCKET_SIZE), fp);
        if (p != 0) { return p; }
        p = searchBottomChain(sb1, fp);
        if (p != 0) { return p; }
        return searchBottomChain(sb2, fp);
    }

    public int size() { return size; }

    public void clear() {
        allocator.close();
        size = 0;
    }

    // ---------------------------------------------------------------------
    //  Internal helpers ----------------------------------------------------------------
    // ---------------------------------------------------------------------

    private long searchAndMaybeUpdate(int index, int fp, long newPtr) {
        long bucketAddr = topBase + ((long) index * BucketLayout.BUCKET_SIZE);
        byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
        if (mask == 0) { return 0; }
        for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
            if ((mask & (1 << slot)) == 0) { continue; }
            int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
            if (tag == fp) {
                long oldPtr = U.getLong(bucketAddr + BucketLayout.ptrOffset(slot));
                U.putLong(bucketAddr + BucketLayout.ptrOffset(slot), newPtr);
                return oldPtr;
            }
        }
        return 0;
    }

    private boolean tryInsertIntoTop(int idx1, int idx2, int fp, long ptr) {
        int target = bucketFreeSlots(idx1) >= bucketFreeSlots(idx2) ? idx1 : idx2;
        return insertIntoBucket(topBase + ((long) target * BucketLayout.BUCKET_SIZE), fp, ptr);
    }

    private boolean insertIntoBucket(long bucketAddr, int fp, long ptr) {
        byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
        if (Integer.bitCount(mask & 0x0F) == BucketLayout.SLOT_COUNT) { return false; }
        int slot = firstZero(mask);
        U.putInt(bucketAddr + BucketLayout.tagOffset(slot), fp);
        U.putLong(bucketAddr + BucketLayout.ptrOffset(slot), ptr);
        U.putByte(bucketAddr + BucketLayout.MASK_OFFSET, (byte) (mask | (1 << slot)));
        return true;
    }

    private int bucketFreeSlots(int idx) {
        byte mask = U.getByte(topBase + ((long) idx * BucketLayout.BUCKET_SIZE) + BucketLayout.MASK_OFFSET);
        return BucketLayout.SLOT_COUNT - Integer.bitCount(mask & 0x0F);
    }

    private boolean relocateOnce(int idx, int fpNew, long ptrNew) {
        long bucketAddr = topBase + ((long) idx * BucketLayout.BUCKET_SIZE);
        byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
        for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
            if ((mask & (1 << slot)) == 0) { continue; }
            int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
            int altIdx = alternativeIndex(tag, idx);
            if (insertIntoBucket(topBase + ((long) altIdx * BucketLayout.BUCKET_SIZE), tag,
                    U.getLong(bucketAddr + BucketLayout.ptrOffset(slot)))) {
                // free slot
                U.putByte(bucketAddr + BucketLayout.MASK_OFFSET, (byte) (mask & ~(1 << slot)));
                return insertIntoBucket(bucketAddr, fpNew, ptrNew);
            }
        }
        return false;
    }

    private int alternativeIndex(int tag, int curIdx) {
        long h2 = HashFunctions.mix64(Long.rotateLeft(tag, 17));
        int idx2 = (int) (h2 & (topCapacity - 1));
        return idx2 == curIdx ? (int) ((HashFunctions.mix64(tag) & (topCapacity - 1))) : idx2;
    }

    private boolean tryInsertIntoBottom(int sbIdx, int fp, long ptr) {
        long bucketAddr = bottomBase + ((long) sbIdx * BucketLayout.BUCKET_SIZE);
        while (true) {
            if (insertIntoBucket(bucketAddr, fp, ptr)) { return true; }
            byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
            boolean hasNext = (mask & (1 << 4)) != 0;
            if (!hasNext) {
                // allocate new chain bucket
                long newAddr = allocateExtraBucket();
                U.putLong(bucketAddr + BucketLayout.PTR3_OFFSET + 8, newAddr); // store as nextPtr
                U.putByte(bucketAddr + BucketLayout.MASK_OFFSET, (byte) (mask | (1 << 4)));
                bucketAddr = newAddr;
            } else {
                bucketAddr = U.getLong(bucketAddr + BucketLayout.PTR3_OFFSET + 8);
            }
        }
    }

    private long allocateExtraBucket() {
        try {
            List<MemorySegment> seg = allocator.allocate(BucketLayout.BUCKET_SIZE);
            ownedSegs.addAll(seg);
            return seg.get(0).getAddress();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    private long searchBucket(long bucketAddr, int fp) {
        byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
        if (mask == 0) { return 0; }
        for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
            if ((mask & (1 << slot)) == 0) { continue; }
            int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
            if (tag == fp) {
                return U.getLong(bucketAddr + BucketLayout.ptrOffset(slot));
            }
        }
        return 0;
    }

    private long searchBottomChain(int sbIdx, int fp) {
        long bucketAddr = bottomBase + ((long) sbIdx * BucketLayout.BUCKET_SIZE);
        while (true) {
            long ptr = searchBucket(bucketAddr, fp);
            if (ptr != 0) { return ptr; }
            byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
            if ((mask & (1 << 4)) == 0) { return 0; }
            bucketAddr = U.getLong(bucketAddr + BucketLayout.PTR3_OFFSET + 8);
        }
    }

    private static int firstZero(byte mask) {
        for (int i = 0; i < BucketLayout.SLOT_COUNT; i++) {
            if ((mask & (1 << i)) == 0) { return i; }
        }
        return -1; // should not reach here
    }

    private long deleteFromBucket(long bucketAddr, int fp) {
        byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
        if (mask == 0) { return 0; }
        for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
            if ((mask & (1 << slot)) == 0) { continue; }
            int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
            if (tag == fp) {
                long ptr = U.getLong(bucketAddr + BucketLayout.ptrOffset(slot));
                // clear bit and zero out ptr
                U.putByte(bucketAddr + BucketLayout.MASK_OFFSET, (byte) (mask & ~(1 << slot)));
                U.putLong(bucketAddr + BucketLayout.ptrOffset(slot), 0);
                return ptr;
            }
        }
        return 0;
    }

    private long deleteFromBottomChain(int sbIdx, int fp) {
        long bucketAddr = bottomBase + ((long) sbIdx * BucketLayout.BUCKET_SIZE);
        while (true) {
            long ptr = deleteFromBucket(bucketAddr, fp);
            if (ptr != 0) { return ptr; }
            byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
            if ((mask & (1 << 4)) == 0) { return 0; }
            bucketAddr = U.getLong(bucketAddr + BucketLayout.PTR3_OFFSET + 8);
        }
    }

    // ---------------------------------------------------------------------
    //  Resize ----------------------------------------------------------------
    // ---------------------------------------------------------------------
    private void resize() throws MemoryAllocationException {
        java.util.List<MemorySegment> oldSegs = new java.util.ArrayList<>(ownedSegs);
        int newTopCap = ResizeHelper.calculateNewTopCapacity(topCapacity);
        long newTopBase = allocateBuckets(newTopCap);
        long newBottomBase = allocateBuckets(newTopCap >> 1);

        // migrate only bottom
        ResizeHelper.migrateBottomToNewTop(this, bottomBase, topCapacity >> 1,
                newTopBase, newTopCap);
        // migrate interim (old top)
        for (int i = 0; i < topCapacity; i++) {
            long bucketAddr = topBase + ((long) i * BucketLayout.BUCKET_SIZE);
            byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
            for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
                if ((mask & (1 << slot)) == 0) { continue; }
                int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
                long ptr = U.getLong(bucketAddr + BucketLayout.ptrOffset(slot));
                rawInsertIntoNewTop(tag, ptr, newTopBase, newTopCap);
            }
        }
        this.topBase = newTopBase;
        this.bottomBase = newBottomBase;
        this.topCapacity = newTopCap;
        // free old segments
        for (MemorySegment seg : oldSegs) {
            if (!ownedSegs.contains(seg)) { // only old
                allocator.free(seg);
            }
        }
    }

    // raw insert into newTop – assumes free slot exists (resize private)
    void rawInsertIntoNewTop(int tag, long ptr, long newTopBase, int newTopCap) {
        int idx = tag & (newTopCap - 1); // simple choice, linear probing if busy
        long bucketAddr = newTopBase + ((long) idx * BucketLayout.BUCKET_SIZE);
        while (!insertIntoBucket(bucketAddr, tag, ptr)) {
            idx = (idx + 1) & (newTopCap - 1);
            bucketAddr = newTopBase + ((long) idx * BucketLayout.BUCKET_SIZE);
        }
    }

    // ---------------------------------------------------------------------
    //  Allocation helpers ---------------------------------------------------
    // ---------------------------------------------------------------------
    private void allocateTables(int topCap) throws MemoryAllocationException {
        this.topBase     = allocateBuckets(topCap);
        this.bottomBase  = allocateBuckets(topCap >> 1);
        this.topCapacity = topCap;
    }

    private long allocateBuckets(int bucketCount) throws MemoryAllocationException {
        int bytes = bucketCount * BucketLayout.BUCKET_SIZE;
        List<MemorySegment> segs = allocator.allocate(bytes);
        ownedSegs.addAll(segs);
        return segs.get(0).getAddress();
    }
    private void freeAllOwned() {
        allocator.free(ownedSegs);
        ownedSegs.clear();
    }

    int topCapacity() {
        return topCapacity;
    }

    /** Release all allocated off‑heap memory. */
    @Override
    public void close() {
        freeAllOwned();
        allocator.close();
    }
}
