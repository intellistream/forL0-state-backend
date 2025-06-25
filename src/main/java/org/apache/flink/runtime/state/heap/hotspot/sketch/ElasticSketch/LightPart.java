package org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch;

import java.util.Arrays;
import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

/**
 * LightPart：使用单哈希、1-byte 计数，上限255。
 */
public class LightPart {
    private final byte[] counters;
    private final Hash hash;
    private final int bucketNum;

    public LightPart() {
        long totalBytes = (long) Config.INSTANCE.totalMemoryKb * 1024;
        long lightBytes = totalBytes - (totalBytes / 4);
        this.counters = new byte[(int) lightBytes];
        this.hash = new Hash(1234);
        this.bucketNum = counters.length;
        clear();
    }

    public void clear() {
        Arrays.fill(counters, (byte) 0);
    }

    public void insert(byte[] key, int f) {
        int h = hash.run(key, key.length);
        int idx = Math.floorMod(h, bucketNum);
        int v = Byte.toUnsignedInt(counters[idx]);
        counters[idx] = (byte) Math.min(v + f, 255);
    }

    public int query(byte[] key) {
        int h = hash.run(key, key.length);
        int idx = Math.floorMod(h, bucketNum);
        return Byte.toUnsignedInt(counters[idx]);
    }

    public void clearKey(byte[] key) {
        int h = hash.run(key, key.length);
        int idx = Math.floorMod(h, bucketNum);
        counters[idx] = 0;
    }
}
