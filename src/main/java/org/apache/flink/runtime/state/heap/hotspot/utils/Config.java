package org.apache.flink.runtime.state.heap.hotspot.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    public static final Config INSTANCE = new Config();
    private final Properties props = new Properties();

    private Config() {
        // Load config.properties, stripping UTF-8 BOM if present
        try (InputStream raw = Config.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (raw == null) {
                throw new RuntimeException("config.properties not found on classpath");
            }
            BufferedInputStream in = new BufferedInputStream(raw);
            in.mark(3);
            // check for BOM 0xEF,0xBB,0xBF
            if (in.read() != 0xEF || in.read() != 0xBB || in.read() != 0xBF) {
                in.reset();  // no BOM, rewind
            }
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }

        // Assign fields from props
        algorithm       = getString("algorithm");
        topK            = getInt("topK");
        traceDir        = getString("traceDir");
        totalMemoryKb   = getInt("totalMemoryKb");
        keyLength4      = getInt("keyLength4");
        counterPerBucket= getInt("counterPerBucket");
        swapMinValThreshold = getInt("swapMinValThreshold");
        highestBitMask  = getLong("highestBitMask");
        hkMemoryKb        = getInt("hkMemoryKb");
        hkDepth         = getInt("hkDepth");
        hkDecay         = getDouble("hkDecay");
        summaryMaxEntries = getInt("summaryMaxEntries");
        summaryChainDepth = getInt("summaryChainDepth");
        summaryHashSlots  = getInt("summaryHashSlots");
    }

    // configuration fields
    public final String algorithm;
    public final int    topK;
    public final String traceDir;

    public final int    totalMemoryKb;
    public final int    keyLength4;
    public final int    counterPerBucket;
    public final int    swapMinValThreshold;
    public final long   highestBitMask;

    public final int    hkMemoryKb;
    public final int    hkDepth;
    public final double hkDecay;

    public final int summaryMaxEntries;
    public final int summaryChainDepth;
    public final int summaryHashSlots;

    private String getString(String key) {
        String v = props.getProperty(key);
        if (v == null) {
            throw new RuntimeException("Missing required config property: " + key);
        }
        return v;
    }

    private int getInt(String key) {
        String v = getString(key);
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer for property '" + key + "': " + v, e);
        }
    }

    private long getLong(String key) {
        String v = getString(key);
        try {
            return Long.decode(v);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid long for property '" + key + "': " + v, e);
        }
    }

    private double getDouble(String key) {
        String v = getString(key);
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid double for property '" + key + "': " + v, e);
        }
    }
}
