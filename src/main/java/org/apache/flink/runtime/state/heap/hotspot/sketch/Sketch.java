package org.apache.flink.runtime.state.heap.hotspot.sketch;

import java.util.List;
import java.util.Map;

/**
 * Sketch 抽象接口，定义了对流元素的插入、查询、清空及批量插入方法，
 * 以及获取 Top-K heavy hitters 的统一方法。
 */
public interface Sketch {
    /** 带权重的插入接口 */
    void insert(byte[] key, int f);

    /** 查询元素的估计频次 */
    int query(byte[] key);

    /** 清空内部状态 */
    void clear();

    /** 批量插入默认实现 */
    default void insertAll(List<byte[]> keys) {
        for (byte[] key : keys) {
            insert(key, 1);
        }
    }

    /**
     * 获取当前估计的 Top-K heavy hitters。
     * @param K       要获取的 heavy hitters 数量
     * @param results 结果列表，实现类需清空并填入不超过 K 条 (元素, 估计值)
     */
    void getHeavyHitters(int K, List<Map.Entry<String, Integer>> results);
}
