package org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch;

import java.util.*;
import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

/**
 * HeavyPart：固定桶 + 多槽 + 前4字节原始指纹存储。
 */
public class HeavyPart {
    private final int bucketNum;
    private final int slotPerBucket;
    private final int fpLen;
    private final Hash hash;

    private final byte[][][] keys;    // [slot][bucket][fpLen]
    private final int[][] counters;   // [slot][bucket]

    public HeavyPart() {
        long totalBytes = (long) Config.INSTANCE.totalMemoryKb * 1024;
        long heavyBytes = totalBytes / 4;
        int counterBytes = Integer.BYTES + Config.INSTANCE.keyLength4;
        this.bucketNum = (int) (heavyBytes / (Config.INSTANCE.counterPerBucket * counterBytes));
        this.slotPerBucket = Config.INSTANCE.counterPerBucket;
        this.fpLen = Config.INSTANCE.keyLength4;
        this.hash = new Hash(5678);

        this.keys = new byte[slotPerBucket][bucketNum][fpLen];
        this.counters = new int[slotPerBucket][bucketNum];
        clear();
    }

    public void clear() {
        for (int i = 0; i < slotPerBucket; i++) {
            Arrays.fill(counters[i], 0);
            // keys can remain, counters==0 indicates empty
        }
    }

    /**
     * 插入：返回 true 表示未在 HeavyPart 命中，需要给 LightPart
     */
    public boolean insert(byte[] key, int f) {
        byte[] fp = Arrays.copyOf(key, fpLen);
        int h = hash.run(key, key.length);
        int idx = Math.floorMod(h, bucketNum);

        // 1) 已有同指纹
        for (int s = 0; s < slotPerBucket; s++) {
            if (counters[s][idx] > 0 && Arrays.equals(keys[s][idx], fp)) {
                counters[s][idx] += f;
                return false;
            }
        }
        // 2) 空槽
        for (int s = 0; s < slotPerBucket; s++) {
            if (counters[s][idx] == 0) {
                System.arraycopy(fp, 0, keys[s][idx], 0, fpLen);
                counters[s][idx] = f;
                return false;
            }
        }
        // 3) 全满，demote
        return true;
    }

    public int query(byte[] key) {
        byte[] fp = Arrays.copyOf(key, fpLen);
        int h = hash.run(key, key.length);
        int idx = Math.floorMod(h, bucketNum);
        int max = 0;
        for (int s = 0; s < slotPerBucket; s++) {
            if (counters[s][idx] > 0 && Arrays.equals(keys[s][idx], fp)) {
                max = Math.max(max, counters[s][idx]);
            }
        }
        return max;
    }

    /**
     * Top-K 提取：聚合所有非零槽，再全局排序。
     */
    public void getHeavyHitters(int K, List<Map.Entry<String,Integer>> results) {
        Map<String,Integer> agg = new HashMap<>();
        for (int s = 0; s < slotPerBucket; s++) {
            for (int j = 0; j < bucketNum; j++) {
                int cnt = counters[s][j];
                if (cnt > 0) {
                    String str = new String(keys[s][j], java.nio.charset.StandardCharsets.ISO_8859_1);
                    agg.merge(str, cnt, Integer::sum);
                }
            }
        }
        List<Map.Entry<String,Integer>> list = new ArrayList<>(agg.entrySet());
        list.sort((a,b) -> Integer.compare(b.getValue(), a.getValue()));
        results.clear();
        for (int i = 0; i < Math.min(K, list.size()); i++) {
            results.add(list.get(i));
        }
    }

    public void swapInsert(byte[] key, int count) {
        insert(key, count);
    }
}