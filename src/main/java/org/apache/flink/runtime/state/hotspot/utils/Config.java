package org.apache.flink.runtime.state.hotspot.utils;

public class Config {
    public static final Config INSTANCE = new Config();

    public int totalMemoryKb = 600;  //

    /** Top-K 阈值 */
    public int topK = 100;

    public static final int COUNTER_PER_BUCKET     = 8;
    public static final int KEY_LENGTH_4           = 4;
    public static final int KEY_LENGTH_13          = 13;
    public static final int SWAP_MIN_VAL_THRESHOLD = 5;
    public static final int HIGHEST_BIT_MASK       = 0x8000_0000;

    /** Heavy 部分桶数 = (totalBytes/4) / (COUNTER_PER_BUCKET * 8) */
    public final int heavyPartBucketNum;
    /** Light 部分可用字节 = totalBytes - totalBytes/4 */
    public final int lightPartBytes;

    private Config() {
        long totalBytes = (long) totalMemoryKb * 1024;      // KB → bytes
        long heavyBytes = totalBytes / 4;                   // C++ heavy_mem = tot/4
        long bucketSize = (long) COUNTER_PER_BUCKET * (KEY_LENGTH_4 * 2); // 8 counters × (4B key+4B val)
        this.heavyPartBucketNum = (int) (heavyBytes / bucketSize);
        this.lightPartBytes     = (int) (totalBytes - heavyBytes);
    }

    /** LightPart 构造时使用的内存（字节数） */
    public int getLightPartBytes() {
        return lightPartBytes;
    }
}

