package org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch;

import org.apache.flink.runtime.state.heap.hotspot.sketch.Sketch;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.util.*;

/**
 * ElasticSketch：整合 HeavyPart & LightPart，动态合并逻辑。
 */
public class ElasticSketch implements Sketch {
    private final HeavyPart heavyPart;
    private final LightPart lightPart;

    public ElasticSketch() {
        this.heavyPart = new HeavyPart();
        this.lightPart = new LightPart();
    }

    @Override
    public void clear() {
        heavyPart.clear();
        lightPart.clear();
    }

    @Override
    public void insert(byte[] key, int f) {
        boolean toLight = heavyPart.insert(key, f);
        if (toLight) {
            lightPart.insert(key, f);
            if (lightPart.query(key) >= Config.INSTANCE.swapMinValThreshold) {
                heavyPart.swapInsert(key, lightPart.query(key));
                lightPart.clearKey(key);
            }
        }
    }

    @Override
    public int query(byte[] key) {
        int h = heavyPart.query(key);
        return h > 0 ? h : lightPart.query(key);
    }

    @Override
    public void insertAll(List<byte[]> flows) {
        for (byte[] buf : flows) insert(buf, 1);
    }

    @Override
    public void getHeavyHitters(int K, List<Map.Entry<String,Integer>> results) {
        heavyPart.getHeavyHitters(K, results);
    }
}
