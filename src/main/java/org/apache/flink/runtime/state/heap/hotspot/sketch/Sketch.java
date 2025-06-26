package org.apache.flink.runtime.state.heap.hotspot.sketch;

import java.util.List;
import java.util.Map;

public interface Sketch {
    void insert(byte[] key, int f);
    int  query(byte[] key);
    void clear();
    default void insertAll(List<byte[]> keys) {
        for (byte[] key : keys) insert(key, 1);
    }
    void getHeavyHitters(int K, List<Map.Entry<String, Integer>> results);
}
