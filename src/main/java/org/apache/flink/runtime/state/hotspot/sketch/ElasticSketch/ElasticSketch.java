package org.apache.flink.runtime.state.hotspot.sketch.ElasticSketch;

import org.apache.flink.runtime.state.hotspot.utils.Config;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Java 版 ElasticSketch，严格对应 C++ ElasticSketch<bucket_num, tot_mem> 的主体逻辑。
 */
public class ElasticSketch {
    private final HeavyPart heavyPart;
    private final LightPart lightPart;

    public static class Pair<K, V> {
        public final K first;
        public final V second;
        public Pair(K f, V s) { first = f; second = s; }
    }

    public ElasticSketch() {
        this.heavyPart = new HeavyPart();
        this.lightPart = new LightPart();
    }

    /** 清空两部分状态 */
    public void clear() {
        heavyPart.clear();
        lightPart.clear();
    }

    /**
     * insert(uint8_t *key, int f=1)
     * 对应 C++ 中的 switch(result) 逻辑：
     *  case 0: return;
     *  case 1: if(highbit) light.insert(); else light.swap_insert(); return;
     *  case 2: light.insert(key,1); return;
     */
    public void insert(byte[] key, int f) {
        byte[] swapKey = new byte[Config.INSTANCE.KEY_LENGTH_4];
        int[] swapVal = new int[1];
        int res = heavyPart.insert(key, swapKey, swapVal, f);

        switch (res) {
            case 0:
                return;
            case 1:
                if ((swapVal[0] & Config.INSTANCE.HIGHEST_BIT_MASK) != 0) {
                    int val = swapVal[0] & ~Config.INSTANCE.HIGHEST_BIT_MASK;
                    lightPart.insert(swapKey, val);
                } else {
                    lightPart.swapInsert(swapKey, swapVal[0]);
                }
                return;
            case 2:
                lightPart.insert(key, 1);
                return;
            default:
                throw new IllegalStateException("Invalid insert result: " + res);
        }
    }

    public void insert(byte[] key) {
        insert(key, 1);
    }

    /**
     * query(uint8_t *key)
     * if (heavy_result == 0 || highbit) return GetCounterVal(heavy_result) + light.query;
     * else return heavy_result;
     */
    public int query(byte[] key) {
        int heavy = heavyPart.query(key);
        boolean missOrSwap = (heavy == 0) || ((heavy & Config.INSTANCE.HIGHEST_BIT_MASK) != 0);
        int hv = heavy & ~Config.INSTANCE.HIGHEST_BIT_MASK;
        if (missOrSwap) {
            return hv + lightPart.query(key);
        } else {
            return hv;
        }
    }

    /**
     * get_heavy_hitters(int threshold, vector<pair<string,int>>& results)
     * 对应 C++: 遍历所有 bucket & MAX_VALID_COUNTER，调用 query(&key)，
     * 排序后取前 threshold 条。
     */
    public void getHeavyHitters(int threshold, List<Pair<String, Integer>> results) {
        results.clear();
        int bucketNum = heavyPart.getBucketNum();
        int per = Config.INSTANCE.COUNTER_PER_BUCKET;

        // 收集所有 (fpBytes, estimate)
        List<Pair<byte[], Integer>> temp = new ArrayList<>(bucketNum * per);
        for (int b = 0; b < bucketNum; b++) {
            for (int i = 0; i < per; i++) {
                int fp = heavyPart.keys[b * per + i];
                // 把 fp 重新转回 4 字节 little-endian
                byte[] fpBytes = ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(fp)
                        .array();
                // 调用 query 对 heavy+light 做统一估计
                int est = query(fpBytes);
                temp.add(new Pair<>(fpBytes, est));
            }
        }

        // 按估计值降序排序
        Collections.sort(temp, Comparator.comparingInt((Pair<byte[], Integer> p) -> p.second).reversed());

        // 取前 threshold 条，转换 keyBytes->String
        int limit = Math.min(threshold, temp.size());
        for (int i = 0; i < limit; i++) {
            byte[] kb = temp.get(i).first;
            String keyStr = new String(kb, 0, 4, java.nio.charset.StandardCharsets.ISO_8859_1);
            results.add(new Pair<>(keyStr, temp.get(i).second));
        }
    }
}
