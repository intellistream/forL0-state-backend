package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

final class ResizeHelper {
    private ResizeHelper() {}

    static int calculateNewTopCapacity(int curTopCap) {
        return curTopCap << 1; // always power‑of‑two doubling
    }

    /**
     * Move all entries in [oldBottom] to the [newTop] table.
     * This is a blocking, single‑thread operation matching Level‑Hashing spec.
     */
    static void migrateBottomToNewTop(OffHeapLevelHashIndex idx,
                                      long oldBottomBase,
                                      int oldBottomBuckets,
                                      long newTopBase,
                                      int newTopCap) {
        Unsafe U = UnsafeUtils.unsafe();
        for (int b = 0; b < oldBottomBuckets; b++) {
            long bucketAddr = oldBottomBase + (long) b * BucketLayout.BUCKET_SIZE;
            byte mask = U.getByte(bucketAddr + BucketLayout.MASK_OFFSET);
            if (mask == 0) { continue; }
            for (int slot = 0; slot < BucketLayout.SLOT_COUNT; slot++) {
                if ((mask & (1 << slot)) == 0) { continue; }
                int tag = U.getInt(bucketAddr + BucketLayout.tagOffset(slot));
                long ptr = U.getLong(bucketAddr + BucketLayout.ptrOffset(slot));
                idx.rawInsertIntoNewTop(tag, ptr, newTopBase, newTopCap);
            }
            // mark bucket empty after move
            U.putByte(bucketAddr + BucketLayout.MASK_OFFSET, (byte) 0);
        }
    }
}
