package org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch;

import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * HeavyPart：严格对应 C++ HeavyPart<bucket_num> 插入逻辑。
 *
 * C++ 逻辑总结：
 * 1. 如果命中（matched），直接累加并返回 0。
 * 2. 否则查找最小槽：
 *    a) 如果最小槽的当前值为 0（空槽），插入新 fp，并返回 0。
 *    b) 否则，如果 minVal + f > SWAP_THRESHOLD，则执行置换：
 *         - 输出被踢出 fp/f 到 swap_key/swap_val，插入新 fp，
 *           并返回 1。
 *    c) 否则（miss 且不置换），返回 2。
 */
public class HeavyPart {
    private static final long CONSTANT_NUMBER = 2654435761L;
    private final int bucketNum = Config.INSTANCE.heavyPartBucketNum;
    private static final int C = Config.INSTANCE.COUNTER_PER_BUCKET;

    // 按桶连续排列的 keys 和 vals
    public final int[] keys;
    public final int[] vals;

    public HeavyPart() {
        keys = new int[bucketNum * C];
        vals = new int[bucketNum * C];
        clear();
    }

    public void clear() {
        Arrays.fill(keys, 0);
        Arrays.fill(vals, 0);
    }

    /**
     * 插入主逻辑
     * @param rawKey     13 字节五元组
     * @param swapKeyOut 长度 4 的缓冲，置换时输出被踢出的 fp（little-endian）
     * @param swapValOut 长度 1 的数组，置换时输出被踢出的计数
     * @param f          增量
     * @return 0=hit or empty-slot insert; 1=swap; 2=miss
     */
    public int insert(byte[] rawKey, byte[] swapKeyOut, int[] swapValOut, int f) {
        // 1) little-endian 解析前 4 字节为 fp
        int fp = ByteBuffer.wrap(rawKey, 0, Config.INSTANCE.KEY_LENGTH_4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        // 2) 乘法散列 >>15，再 mod 桶数
        int bid = (int)(((Integer.toUnsignedLong(fp) * CONSTANT_NUMBER) >>> 15) % bucketNum);
        int base = bid * C;

        // 3) 查找是否命中
        for (int i = 0; i < C; i++) {
            int idx = base + i;
            if (keys[idx] == fp) {
                // 命中 heavy update
                vals[idx] += f;
                return 0;
            }
        }

        // 4) 查找最小槽
        int minIdx = base;
        int minVal = Integer.MAX_VALUE;
        for (int i = 0; i < C; i++) {
            int idx = base + i;
            int v = vals[idx] & ~Config.INSTANCE.HIGHEST_BIT_MASK;
            if (v < minVal) {
                minVal = v;
                minIdx = idx;
            }
        }

        // 5a) 空槽插入
        if (minVal == 0) {
            keys[minIdx] = fp;
            vals[minIdx] = f;
            return 0;
        }

        // 5b) 满足置换条件
        if (minVal + f > Config.INSTANCE.SWAP_MIN_VAL_THRESHOLD) {
            // 输出被踢出的 key/val
            int oldKey = keys[minIdx];
            int oldVal = vals[minIdx] & ~Config.INSTANCE.HIGHEST_BIT_MASK;
            ByteBuffer.wrap(swapKeyOut)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(oldKey);
            swapValOut[0] = oldVal;
            // 插入新 fp，设置高位标记
            keys[minIdx] = fp;
            vals[minIdx] = (minVal + f) | Config.INSTANCE.HIGHEST_BIT_MASK;
            return 1;
        }

        // 5c) miss 不置换
        return 2;
    }

    /**
     * 快速插入：只更新已有或空槽/最小槽，不执行置换逻辑
     */
    public int quickInsert(byte[] rawKey, int f) {
        // 调用 insert，但忽略置换信号
        byte[] dummyKey = new byte[Config.INSTANCE.KEY_LENGTH_4];
        int[] dummyVal = new int[1];
        int r = insert(rawKey, dummyKey, dummyVal, f);
        // 如果 r==1 或 2，相当于 miss，我们在 quickInsert 中也插入到最小槽
        if (r != 0) {
            // 重复最小槽插入逻辑
            int fp = ByteBuffer.wrap(rawKey, 0, Config.INSTANCE.KEY_LENGTH_4)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
            int bid = (int)(((Integer.toUnsignedLong(fp) * CONSTANT_NUMBER) >>> 15) % bucketNum);
            int base = bid * C;
            int minIdx = base;
            int minVal = Integer.MAX_VALUE;
            for (int i = 0; i < C; i++) {
                int idx = base + i;
                int v = vals[idx] & ~Config.INSTANCE.HIGHEST_BIT_MASK;
                if (v < minVal) {
                    minVal = v;
                    minIdx = idx;
                }
            }
            keys[minIdx] = fp;
            vals[minIdx] = minVal + f;
        }
        return r == 0 ? 0 : 2;
    }

    /**
     * query 查询，返回带最高位标记的 raw val
     */
    public int query(byte[] rawKey) {
        int fp = ByteBuffer.wrap(rawKey, 0, Config.INSTANCE.KEY_LENGTH_4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        int bid = (int)(((Integer.toUnsignedLong(fp) * CONSTANT_NUMBER) >>> 15) % bucketNum);
        int base = bid * C;
        for (int i = 0; i < C; i++) {
            int idx = base + i;
            if (keys[idx] == fp) {
                return vals[idx];
            }
        }
        return 0;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public int getMemoryUsage() {
        return bucketNum * C * Integer.BYTES * 2;
    }
}
