package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.runtime.state.heap.ForL0StateBackendOptions.*;

/**
 * Holds the configuration for ForL0StateBackend.
 * This class parses and validates configuration from Flink's ReadableConfig
 * and provides type-safe access to all configuration values.
 *
 * <p>This class is Serializable to support transmission between TaskManagers.
 */
public class ForL0StateBackendConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackendConfig.class);

    // ========== Configuration Values ==========

    /** Whether L0 cache is enabled. */
    private final boolean l0CacheEnabled;

    /** Size of L0Table as power of 2 (bucket count = 2^size). */
    private final int l0CacheSize;

    /** Replacement policy for L0 cache. */
    private final L0Table.ReplacementPolicy l0ReplacementPolicy;

    /** Maximum L0 memory pool capacity in bytes. -1 means unlimited. */
    private final long l0MemoryMaxBytes;

    /** Initial size of MainTable as power of 2 (bucket count = 2^size). */
    private final int mainTableInitialSize;

    /** Load factor threshold for MainTable resize. */
    private final double mainTableLoadFactorThreshold;

    /**
     * Creates a default configuration with all default values.
     */
    public ForL0StateBackendConfig() {
        this.l0CacheEnabled = L0_CACHE_ENABLED.defaultValue();
        this.l0CacheSize = L0_CACHE_SIZE.defaultValue();
        this.l0ReplacementPolicy = parseReplacementPolicy(L0_CACHE_REPLACEMENT_POLICY.defaultValue());
        this.l0MemoryMaxBytes = -1; // unlimited
        this.mainTableInitialSize = MAIN_TABLE_INITIAL_SIZE.defaultValue();
        this.mainTableLoadFactorThreshold = MAIN_TABLE_LOAD_FACTOR_THRESHOLD.defaultValue();

        LOG.debug("ForL0StateBackendConfig created with default values: {}", this);
    }

    /**
     * Creates a configuration by parsing values from Flink's ReadableConfig.
     *
     * @param config The Flink configuration to read from
     * @throws IllegalArgumentException if any configuration value is invalid
     */
    public ForL0StateBackendConfig(ReadableConfig config) {
        // Parse L0 cache settings
        this.l0CacheEnabled = config.get(L0_CACHE_ENABLED);

        int cacheSize = config.get(L0_CACHE_SIZE);
        validateL0CacheSize(cacheSize);
        this.l0CacheSize = cacheSize;

        String policyStr = config.get(L0_CACHE_REPLACEMENT_POLICY);
        this.l0ReplacementPolicy = parseReplacementPolicy(policyStr);

        // Parse L0 memory pool settings
        MemorySize memorySize = config.get(L0_MEMORY_MAX_SIZE);
        this.l0MemoryMaxBytes = (memorySize == null || memorySize.getBytes() <= 0) ? -1 : memorySize.getBytes();

        // Parse MainTable settings
        int mainTableSize = config.get(MAIN_TABLE_INITIAL_SIZE);
        validateMainTableInitialSize(mainTableSize);
        this.mainTableInitialSize = mainTableSize;

        double loadFactor = config.get(MAIN_TABLE_LOAD_FACTOR_THRESHOLD);
        validateLoadFactorThreshold(loadFactor);
        this.mainTableLoadFactorThreshold = loadFactor;

        LOG.info("ForL0StateBackendConfig created from config: {}", this);
    }

    /**
     * Private constructor for builder pattern or copying.
     */
    private ForL0StateBackendConfig(
            boolean l0CacheEnabled,
            int l0CacheSize,
            L0Table.ReplacementPolicy l0ReplacementPolicy,
            long l0MemoryMaxBytes,
            int mainTableInitialSize,
            double mainTableLoadFactorThreshold) {
        this.l0CacheEnabled = l0CacheEnabled;
        this.l0CacheSize = l0CacheSize;
        this.l0ReplacementPolicy = l0ReplacementPolicy;
        this.l0MemoryMaxBytes = l0MemoryMaxBytes;
        this.mainTableInitialSize = mainTableInitialSize;
        this.mainTableLoadFactorThreshold = mainTableLoadFactorThreshold;
    }

    // ========== Getters ==========

    public boolean isL0CacheEnabled() {
        return l0CacheEnabled;
    }

    public int getL0CacheSize() {
        return l0CacheSize;
    }

    public L0Table.ReplacementPolicy getL0ReplacementPolicy() {
        return l0ReplacementPolicy;
    }

    /**
     * Gets the maximum L0 memory pool capacity in bytes.
     *
     * @return Maximum capacity in bytes, or -1 if unlimited
     */
    public long getL0MemoryMaxBytes() {
        return l0MemoryMaxBytes;
    }

    public int getMainTableInitialSize() {
        return mainTableInitialSize;
    }

    public double getMainTableLoadFactorThreshold() {
        return mainTableLoadFactorThreshold;
    }

    // ========== Utility Methods ==========

    /**
     * Creates a new config with L0 cache disabled.
     * Other values are copied from this config.
     */
    public ForL0StateBackendConfig withL0CacheDisabled() {
        return new ForL0StateBackendConfig(
                false,
                this.l0CacheSize,
                this.l0ReplacementPolicy,
                this.l0MemoryMaxBytes,
                this.mainTableInitialSize,
                this.mainTableLoadFactorThreshold);
    }

    /**
     * Creates a Builder for programmatic configuration.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ForL0StateBackendConfig{" +
                "l0CacheEnabled=" + l0CacheEnabled +
                ", l0CacheSize=" + l0CacheSize + " (=" + (1 << l0CacheSize) + " buckets)" +
                ", l0ReplacementPolicy=" + l0ReplacementPolicy +
                ", l0MemoryMaxBytes=" + (l0MemoryMaxBytes == -1 ? "unlimited" : l0MemoryMaxBytes + " bytes") +
                ", mainTableInitialSize=" + mainTableInitialSize + " (=" + (1 << mainTableInitialSize) + " buckets)" +
                ", mainTableLoadFactorThreshold=" + mainTableLoadFactorThreshold +
                '}';
    }

    // ========== Builder ==========

    /**
     * Builder for programmatic configuration.
     */
    public static class Builder {
        private boolean l0CacheEnabled = L0_CACHE_ENABLED.defaultValue();
        private int l0CacheSize = L0_CACHE_SIZE.defaultValue();
        private L0Table.ReplacementPolicy l0ReplacementPolicy = L0Table.ReplacementPolicy.CLOCK;
        private long l0MemoryMaxBytes = -1;
        private int mainTableInitialSize = MAIN_TABLE_INITIAL_SIZE.defaultValue();
        private double mainTableLoadFactorThreshold = MAIN_TABLE_LOAD_FACTOR_THRESHOLD.defaultValue();

        public Builder setL0CacheEnabled(boolean enabled) {
            this.l0CacheEnabled = enabled;
            return this;
        }

        public Builder setL0CacheSize(int size) {
            validateL0CacheSize(size);
            this.l0CacheSize = size;
            return this;
        }

        public Builder setL0ReplacementPolicy(L0Table.ReplacementPolicy policy) {
            this.l0ReplacementPolicy = policy;
            return this;
        }

        public Builder setL0ReplacementPolicy(String policyString) {
            this.l0ReplacementPolicy = parseReplacementPolicy(policyString);
            return this;
        }

        public Builder setL0MemoryMaxBytes(long bytes) {
            this.l0MemoryMaxBytes = bytes;
            return this;
        }

        public Builder setL0MemoryMaxSize(MemorySize memorySize) {
            this.l0MemoryMaxBytes = (memorySize == null || memorySize.getBytes() <= 0) ? -1 : memorySize.getBytes();
            return this;
        }

        public Builder setMainTableInitialSize(int size) {
            validateMainTableInitialSize(size);
            this.mainTableInitialSize = size;
            return this;
        }

        public Builder setMainTableLoadFactorThreshold(double threshold) {
            validateLoadFactorThreshold(threshold);
            this.mainTableLoadFactorThreshold = threshold;
            return this;
        }

        public ForL0StateBackendConfig build() {
            return new ForL0StateBackendConfig(
                    l0CacheEnabled,
                    l0CacheSize,
                    l0ReplacementPolicy,
                    l0MemoryMaxBytes,
                    mainTableInitialSize,
                    mainTableLoadFactorThreshold);
        }
    }
}
