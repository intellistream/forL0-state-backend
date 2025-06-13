package org.apache.flink.runtime.state.heap.hotspot.sketch.ElasticSketch;

import org.apache.flink.runtime.state.heap.hotspot.hash.Hash;
import org.apache.flink.runtime.state.heap.hotspot.utils.Config;

import java.util.Arrays;
import java.util.List;

/**
 * LightPart 实现，严格对应 C++ LightPart<init_mem_in_bytes> 接口与逻辑。:contentReference[oaicite:0]{index=0}
 */
public class LightPart {
    /** 桶数 = init_mem_in_bytes */
    private final int counterNum;
    /** 计数器数组 */
    private final byte[] counters;
    /** 频次分布数组 */
    private final int[] miceDist;
    /** 对应 C++ 中的 BOBHash32 实例 */
    private final Hash bobhash;

    /**
     * 构造：counterNum = Config.getLightPartBytes(), 并用默认 seed 初始化 BOBHash32
     */
    public LightPart() {
        this.counterNum = Config.INSTANCE.getLightPartBytes();
        this.counters = new byte[counterNum];
        this.miceDist = new int[256];
        Arrays.fill(miceDist, 0);
        // 对应 C++ LightPart() 中 new BOBHash32(rd() % MAX_PRIME32);
        // 无需随机 seed，直接使用默认构造
        this.bobhash = new Hash();
        clear();
    }

    /** 清空所有计数器和分布 */
    public void clear() {
        Arrays.fill(counters, (byte) 0);
        Arrays.fill(miceDist, 0);
    }

    /**
     * 插入，对应 C++ void insert(uint8_t* key, int f=1)
     */
    public void insert(byte[] key, int f) {
        int hashVal = bobhash.run(key, Config.INSTANCE.KEY_LENGTH_4);
        int pos = Math.floorMod(hashVal, counterNum);
        int oldVal = Byte.toUnsignedInt(counters[pos]);
        int newVal = oldVal + f;
        if (newVal > 255) newVal = 255;
        counters[pos] = (byte) newVal;
        miceDist[oldVal]--;
        miceDist[newVal]++;
    }

    /** f=1 的重载 */
    public void insert(byte[] key) {
        insert(key, 1);
    }

    /**
     * 置换式插入，对应 C++ void swap_insert(uint8_t* key, int f)
     */
    public void swapInsert(byte[] key, int f) {
        int hashVal = bobhash.run(key, Config.INSTANCE.KEY_LENGTH_4);
        int pos = Math.floorMod(hashVal, counterNum);
        int capped = f < 255 ? f : 255;
        int oldVal = Byte.toUnsignedInt(counters[pos]);
        if (capped > oldVal) {
            counters[pos] = (byte) capped;
            miceDist[oldVal]--;
            miceDist[capped]++;
        }
    }

    /**
     * 查询，对应 C++ int query(uint8_t* key)
     */
    public int query(byte[] key) {
        int hashVal = bobhash.run(key, Config.INSTANCE.KEY_LENGTH_4);
        int pos = Math.floorMod(hashVal, counterNum);
        return Byte.toUnsignedInt(counters[pos]);
    }

    /**
     * 压缩输出，对应 C++ void compress(int ratio, uint8_t* dst)
     */
    public void compress(int ratio, byte[] dst) {
        int width = getCompressWidth(ratio);
        for (int i = 0; i < width; i++) {
            byte maxVal = 0;
            for (int j = i; j < counterNum; j += width) {
                if (counters[j] > maxVal) {
                    maxVal = counters[j];
                }
            }
            dst[i] = maxVal;
        }
    }

    /**
     * 查询压缩后部分，对应 C++ int query_compressed_part(...)
     */
    public int queryCompressedPart(byte[] key, byte[] compressPart, int compressCounterNum) {
        int hashVal = bobhash.run(key, Config.INSTANCE.KEY_LENGTH_4);
        int pos = Math.floorMod(hashVal, counterNum) % compressCounterNum;
        return Byte.toUnsignedInt(compressPart[pos]);
    }

    /** 返回压缩后槽数，对应 C++ get_compress_width */
    public int getCompressWidth(int ratio) {
        return counterNum / ratio;
    }

    /** 返回压缩后内存，对应 C++ get_compress_memory */
    public int getCompressMemory(int ratio) {
        return getCompressWidth(ratio);
    }

    /** 内存使用字节数，对应 C++ get_memory_usage */
    public int getMemoryUsage() {
        return counterNum;
    }

    /** 基数估计，对应 C++ get_cardinality */
    public int getCardinality() {
        int miceCard = 0;
        for (int i = 1; i < 256; i++) {
            miceCard += miceDist[i];
        }
        double rate = (counterNum - miceCard) / (double) counterNum;
        return (int) (counterNum * Math.log(1.0 / rate));
    }

    /** 熵估计，对应 C++ double get_entropy() */
    public double getEntropy() {
        int tot = 0;
        double entr = 0;
        for (int i = 1; i < 256; i++) {
            int cnt = miceDist[i];
            tot += cnt * i;
            entr += cnt * i * (Math.log(i) / Math.log(2));
        }
        return -entr / tot + Math.log(tot) / Math.log(2);
    }

    /**
     * 分步输出熵估计，对应 C++ void get_entropy(int& tot, double& entr)
     */
    public void getEntropy(int[] totOut, double[] entrOut) {
        int tot = 0;
        double entr = 0;
        for (int i = 1; i < 256; i++) {
            int cnt = miceDist[i];
            tot += cnt * i;
            entr += cnt * i * (Math.log(i) / Math.log(2));
        }
        totOut[0] = tot;
        entrOut[0] = -entr / tot + Math.log(tot) / Math.log(2);
    }

    /**
     * 分布估计，对应 C++ void get_distribution(vector<double>& dist)
     */
    public void getDistribution(List<Double> dist) {
        dist.clear();
        for (int i = 1; i < 256; i++) {
            int cnt = miceDist[i];
            if (cnt == 0) continue;
            while (dist.size() <= i) {
                dist.add(0.0);
            }
            dist.set(i, dist.get(i) + cnt);
        }
    }
}
