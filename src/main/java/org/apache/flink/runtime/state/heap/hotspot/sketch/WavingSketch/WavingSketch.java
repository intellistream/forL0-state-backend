package org.apache.flink.runtime.state.heap.hotspot.sketch.WavingSketch;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.flink.runtime.state.heap.hotspot.sketch.Sketch;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

public class WavingSketch implements Sketch {
    private final int SLOT_NUM;
    private final int BUCKET_NUM;
    private final Bucket[] buckets;
    private static final int[] count = {1, -1};

    /**
     * 无参构造，使用 Config 中配置的 slotNum 和 bucketNum
     */
    public WavingSketch() {
        this(Config.INSTANCE.wavingSlotNum, Config.INSTANCE.wavingBucketNum);
    }

    /**
     * 全参数构造
     */
    public WavingSketch(int slotNum, int bucketNum) {
        this.SLOT_NUM   = slotNum;
        this.BUCKET_NUM = bucketNum;
        this.buckets    = new Bucket[bucketNum];
        for (int i = 0; i < bucketNum; i++) {
            buckets[i] = new Bucket(slotNum);
        }
    }

    @Override
    public void clear() {
        for (Bucket b : buckets) b.clear();
    }

    @Override
    public void insert(byte[] key, int f) {
        int fpLen = Config.INSTANCE.keyLength4;
        String s = new String(key, 0, Math.min(fpLen, key.length),
                StandardCharsets.ISO_8859_1);
        int hash = Murmur.hash(s.hashCode(), 17) & 0x7fffffff;
        int idx  = hash % BUCKET_NUM;
        for (int i = 0; i < f; i++) {
            buckets[idx].insert(s);
        }
    }

    @Override
    public int query(byte[] key) {
        int fpLen = Config.INSTANCE.keyLength4;
        String s = new String(key, 0, Math.min(fpLen, key.length),
                StandardCharsets.ISO_8859_1);
        int hash = Murmur.hash(s.hashCode(), 17) & 0x7fffffff;
        int idx  = hash % BUCKET_NUM;
        return buckets[idx].query(s);
    }

    @Override
    public void getHeavyHitters(int K, List<Map.Entry<String, Integer>> results) {
        Map<String,Integer> agg = new HashMap<>();
        for (Bucket b : buckets) {
            for (Map.Entry<String,Integer> e : b.entries()) {
                agg.merge(e.getKey(), e.getValue(), Integer::sum);
            }
        }
        List<Map.Entry<String,Integer>> all = new ArrayList<>(agg.entrySet());
        all.sort((a,b) -> Integer.compare(b.getValue(), a.getValue()));
        results.clear();
        for (int i = 0; i < Math.min(K, all.size()); i++) {
            Map.Entry<String,Integer> e = all.get(i);
            results.add(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()));
        }
    }

    // -------- 内部 Bucket 类 --------
    private static class Bucket {
        private final String[] items;
        private final int[]    counters;
        private int incast;

        public Bucket(int slotNum) {
            this.items    = new String[slotNum];
            this.counters = new int[slotNum];
            this.incast   = 0;
        }

        public void insert(String key) {
            int choice = Murmur.hash(key.hashCode(), 17) & 1;
            for (int i = 0; i < items.length; i++) {
                if (key.equals(items[i])) {
                    counters[i] += count[choice];
                    incast       += count[choice];
                    return;
                }
            }
            int minPos = 0, minVal = Math.abs(counters[0]);
            for (int i = 1; i < items.length; i++) {
                int v = Math.abs(counters[i]);
                if (v < minVal) { minVal = v; minPos = i; }
            }
            if ((incast ^ counters[minPos]) >= 0 || minVal == 0) {
                items[minPos]    = key;
                counters[minPos] = (minVal == 0
                        ? count[choice]
                        : counters[minPos] + count[choice]);
            }
            incast += count[choice];
        }

        public int query(String key) {
            for (int i = 0; i < items.length; i++) {
                if (key.equals(items[i])) {
                    return Math.abs(counters[i]);
                }
            }
            return 0;
        }

        public List<Map.Entry<String,Integer>> entries() {
            List<Map.Entry<String,Integer>> list = new ArrayList<>();
            for (int i = 0; i < items.length; i++) {
                if (items[i] != null) {
                    list.add(new AbstractMap.SimpleEntry<>(
                            items[i], Math.abs(counters[i])));
                }
            }
            return list;
        }

        public void clear() {
            Arrays.fill(items, null);
            Arrays.fill(counters, 0);
            incast = 0;
        }
    }
}

