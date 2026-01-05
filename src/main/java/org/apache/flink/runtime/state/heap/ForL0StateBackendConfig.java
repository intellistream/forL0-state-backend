package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.flink.runtime.state.heap.ForL0StateBackendOptions.*;

/**
 * Holds the configuration for ForL0StateBackend with SwissMap architecture.
 * 
 * <p>The new SwissMap architecture uses adaptive directory expansion and
 * requires minimal configuration. Most options from the previous design
 * are deprecated but kept for backward compatibility.
 *
 * <p>This class is Serializable to support transmission between TaskManagers.
 */
public class ForL0StateBackendConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackendConfig.class);

    // ========== Configuration Values ==========

    /** Maximum native memory pool capacity in bytes. -1 means unlimited. */
    private final long l0MemoryMaxBytes;

    /**
     * Creates a default configuration with all default values.
     */
    public ForL0StateBackendConfig() {
        this.l0MemoryMaxBytes = -1; // unlimited
        LOG.debug("ForL0StateBackendConfig created with default values: {}", this);
    }

    /**
     * Creates a configuration by parsing values from Flink's ReadableConfig.
     *
     * @param config The Flink configuration to read from
     */
    public ForL0StateBackendConfig(ReadableConfig config) {
        // Parse native memory pool settings
        MemorySize memorySize = config.get(L0_MEMORY_MAX_SIZE);
        this.l0MemoryMaxBytes = (memorySize == null || memorySize.getBytes() <= 0) ? -1 : memorySize.getBytes();

        LOG.info("ForL0StateBackendConfig created from config: {}", this);
    }

    /**
     * Private constructor for builder pattern.
     */
    private ForL0StateBackendConfig(long l0MemoryMaxBytes) {
        this.l0MemoryMaxBytes = l0MemoryMaxBytes;
    }

    // ========== Getters ==========

    /**
     * Gets the maximum native memory pool capacity in bytes.
     *
     * @return Maximum capacity in bytes, or -1 if unlimited
     */
    public long getL0MemoryMaxBytes() {
        return l0MemoryMaxBytes;
    }

    // ========== Deprecated getters for backward compatibility ==========

    /**
     * @deprecated Always returns true. L0 cache concept removed in SwissMap architecture.
     */
    @Deprecated
    public boolean isL0CacheEnabled() {
        return true;
    }

    /**
     * @deprecated Returns default value. L0 cache concept removed in SwissMap architecture.
     */
    @Deprecated
    public int getL0CacheSize() {
        return L0_CACHE_SIZE.defaultValue();
    }

    /**
     * @deprecated Returns default value. No effect in SwissMap architecture.
     */
    @Deprecated
    public double getMainTableLoadFactorThreshold() {
        return MAIN_TABLE_LOAD_FACTOR_THRESHOLD.defaultValue();
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
                "l0MemoryMaxBytes=" + (l0MemoryMaxBytes == -1 ? "unlimited" : l0MemoryMaxBytes + " bytes") +
                '}';
    }

    // ========== Builder ==========

    /**
     * Builder for programmatic configuration.
     */
    public static class Builder {
        private long l0MemoryMaxBytes = -1;

        public Builder setL0MemoryMaxBytes(long bytes) {
            this.l0MemoryMaxBytes = bytes;
            return this;
        }

        public Builder setL0MemoryMaxSize(MemorySize memorySize) {
            this.l0MemoryMaxBytes = (memorySize == null || memorySize.getBytes() <= 0) ? -1 : memorySize.getBytes();
            return this;
        }

        // ========== Deprecated setters for backward compatibility ==========

        /**
         * @deprecated No effect in SwissMap architecture.
         */
        @Deprecated
        public Builder setL0CacheEnabled(boolean enabled) {
            // No-op, kept for backward compatibility
            return this;
        }

        /**
         * @deprecated No effect in SwissMap architecture.
         */
        @Deprecated
        public Builder setL0CacheSize(int size) {
            // No-op, kept for backward compatibility
            return this;
        }

        /**
         * @deprecated No effect in SwissMap architecture.
         */
        @Deprecated
        public Builder setMainTableLoadFactorThreshold(double threshold) {
            // No-op, kept for backward compatibility
            return this;
        }

        public ForL0StateBackendConfig build() {
            return new ForL0StateBackendConfig(l0MemoryMaxBytes);
        }
    }
}
