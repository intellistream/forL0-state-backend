package org.apache.flink.runtime.state.heap.levelhash;

import org.apache.flink.runtime.state.heap.utils.UnsafeUtils;
import sun.misc.Unsafe;

final class LevelHashResizeHelper {
    private LevelHashResizeHelper() {}

    static int calculateNewTopCapacity(int curTopCap) {
        return curTopCap << 1; // always power‑of‑two doubling
    }

    /**
     * Move all entries in [oldBottom] to the [newTop] table.
     * This is a blocking, single‑thread operation matching Level‑Hashing spec.
     */
    static void migrateBottomToNewTop(LevelHashIndex idx,
                                      long oldBottomBase,
                                      int oldBottomBuckets,
                                      long newTopBase,
                                      int newTopCap) {
        Unsafe U = UnsafeUtils.unsafe();
        for (int b = 0; b < oldBottomBuckets; b++) {
            long bucketAddr = oldBottomBase + (long) b * LevelHashBucketLayout.BUCKET_SIZE;
            byte mask = U.getByte(bucketAddr + LevelHashBucketLayout.MASK_OFFSET);
            if (mask == 0) { continue; }
            for (int slot = 0; slot < LevelHashBucketLayout.SLOT_COUNT; slot++) {
                if ((mask & (1 << slot)) == 0) { continue; }
                int tag = U.getInt(bucketAddr + LevelHashBucketLayout.tagOffset(slot));
                long ptr = U.getLong(bucketAddr + LevelHashBucketLayout.ptrOffset(slot));
                idx.rawInsertIntoNewTop(tag, ptr, newTopBase, newTopCap);
            }
            // mark bucket empty after move
            U.putByte(bucketAddr + LevelHashBucketLayout.MASK_OFFSET, (byte) 0);
        }
    }
}
