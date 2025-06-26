package org.apache.flink.runtime.state.heap.hotspot.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Singleton configuration loader.
 * WavingSketch bucket count is derived from memoryKb, slotNum and slotSizeBytes.
 */
public class Config {
    public static final Config INSTANCE = new Config();
    private final Properties props = new Properties();

    // General settings
    public final String  algorithm;
    public final int     topK;
    public final String  traceDir;
    public final int     keyLength4;
    public final long    highestBitMask;
    public final int     counterPerBucket;
    public final int     swapMinValThreshold;
    public final int     totalMemoryKb;

    // HeavyKeeper settings
    public final int     hkMemoryKb;
    public final int     hkDepth;
    public final double  hkDecay;

    // Stream-Summary settings
    public final int     summaryMaxEntries;
    public final int     summaryChainDepth;
    public final int     summaryHashSlots;

    // WavingSketch settings
    public final int     wavingMemoryKb;
    public final int     wavingSlotNum;
    public final int     wavingSlotSizeBytes;
    public final int     wavingBucketNum;

    private Config() {
        try (InputStream raw = Config.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (raw == null) {
                throw new RuntimeException("config.properties not found");
            }
            BufferedInputStream in = new BufferedInputStream(raw);
            in.mark(3);
            // Remove BOM if present
            if (in.read() != 0xEF || in.read() != 0xBB || in.read() != 0xBF) {
                in.reset();
            }
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }

        // Load general settings
        algorithm          = getString("algorithm");
        topK               = getInt("topK");
        traceDir           = getString("traceDir");
        keyLength4         = getInt("keyLength4");
        highestBitMask     = getLong("highestBitMask");
        counterPerBucket   = getInt("counterPerBucket");
        swapMinValThreshold= getInt("swapMinValThreshold");
        totalMemoryKb      = getInt("totalMemoryKb");

        // Load HeavyKeeper settings
        hkMemoryKb = getInt("hkMemoryKb");
        hkDepth    = getInt("hkDepth");
        hkDecay    = getDouble("hkDecay");

        // Load Stream-Summary settings
        summaryMaxEntries = getInt("summaryMaxEntries");
        summaryChainDepth = getInt("summaryChainDepth");
        summaryHashSlots  = getInt("summaryHashSlots");

        // Load WavingSketch settings
        wavingMemoryKb     = getInt("wavingSketch.memoryKb");
        wavingSlotNum      = getInt("wavingSketch.slotNum");
        wavingSlotSizeBytes= getInt("wavingSketch.slotSizeBytes");

        // Compute bucket count so that:
        // bucketNum * slotNum * slotSizeBytes <= memoryKb * 1024
        long totalBytes     = (long) wavingMemoryKb * 1024;
        long bytesPerBucket = (long) wavingSlotNum * wavingSlotSizeBytes;
        int buckets = (int) (totalBytes / bytesPerBucket);
        if (buckets <= 0) {
            throw new IllegalArgumentException(
                    String.format("WavingSketch: memoryKb=%d too small for slotNum=%d and slotSizeBytes=%d",
                            wavingMemoryKb, wavingSlotNum, wavingSlotSizeBytes));
        }
        wavingBucketNum = buckets;
    }

    private String getString(String key) {
        String v = props.getProperty(key);
        if (v == null) {
            throw new RuntimeException("Missing config property: " + key);
        }
        return v;
    }

    private int getInt(String key) {
        try {
            return Integer.parseInt(getString(key));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer for property: " + key);
        }
    }

    private long getLong(String key) {
        try {
            return Long.decode(getString(key));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid long for property: " + key);
        }
    }

    private double getDouble(String key) {
        try {
            return Double.parseDouble(getString(key));
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid double for property: " + key);
        }
    }
}