package org.apache.flink.runtime.state.hotspot.sketch;

/**
 * Sketch 抽象基类，定义所有 Sketch 算法应实现的基本接口和通用操作。
 */
public abstract class Sketch {
    /**
     * 插入一个 key（通常为原始字节数组表示的流标识 / 5-tuple）。
     * 各算法按各自逻辑更新内部计数结构。
     * @param keyBytes key 的二进制表示
     */
    public abstract void insert(byte[] keyBytes);

    /**
     * 查询给定 key 的估计计数。
     * 对于 HeavyPart+LightPart 结构，若 HeavyPart 命中则返回精确计数，否则返回 LightPart 估计。
     * @param keyBytes key 的二进制表示
     * @return 该 key 的估计出现次数
     */
    public abstract long query(byte[] keyBytes);

    /**
     * 清空内部状态，将 Sketch 重置到初始状态。
     * 可选实现；某些算法可能不支持或不需要清空。
     */
    public void reset() {
        // 默认由子类覆盖，如需实现
        throw new UnsupportedOperationException("reset() not implemented");
    }

    /**
     * 批量插入一组 keys，默认按顺序逐个调用 insert()。
     * 子类可覆盖以做批量优化。
     * @param keysIterable 可迭代的 keyBytes 数组集合
     */
    public void insertAll(Iterable<byte[]> keysIterable) {
        for (byte[] key : keysIterable) {
            insert(key);
        }
    }
}
