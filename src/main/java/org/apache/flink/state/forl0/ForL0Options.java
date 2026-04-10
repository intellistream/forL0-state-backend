package org.apache.flink.state.forl0;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/**
 * Configuration options for ForL0StateBackend.
 */
@PublicEvolving
public class ForL0Options {

    // ========== StateBackend Type ==========

    /** The identifier for ForL0StateBackend in configuration. */
    public static final String STATE_BACKEND_TYPE = "forl0";

    // ========== SwissTable Options ==========

    public static final ConfigOption<Integer> INITIAL_TABLE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.initial-table-capacity")
                    .intType()
                    .defaultValue(64)
                    .withDescription("Initial capacity of each SwissTable. Must be a power of 2 and >= 8.");

    public static final ConfigOption<Integer> MAX_TABLE_CAPACITY =
            ConfigOptions.key("state.backend.forl0.max-table-capacity")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum capacity of each SwissTable before triggering split. Must be a power of 2.");

    // ========== Snapshot Options ==========

    public static final ConfigOption<Boolean> ASYNC_SNAPSHOTS =
            ConfigOptions.key("state.backend.forl0.async-snapshots")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use asynchronous snapshots.");

    // ========== L0 Memory Options ==========

    public static final ConfigOption<Boolean> L0_CACHE_ENABLED =
            ConfigOptions.key("state.backend.forl0.l0-cache.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable L0 Cache memory allocation. " +
                            "This requires the native library and Kunpeng CPU with L0 support.");

    public static final ConfigOption<MemorySize> L0_CACHE_SIZE =
            ConfigOptions.key("state.backend.forl0.l0-cache.size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription("Size of L0 Cache memory pool. Examples: 256mb, 1gb.");

    public static final ConfigOption<MemorySize> L0_CACHE_MAX_PER_ALLOC =
            ConfigOptions.key("state.backend.forl0.l0-cache.max-per-alloc")
                    .memoryType()
                    .defaultValue(new MemorySize(64 * 1024))
                    .withDescription("Maximum single allocation size that can use L0 memory. " +
                            "Allocations larger than this threshold will always use heap memory. Examples: 64kb, 128kb.");

    // ========== Internal Constants ==========

    /** Minimum table capacity. */
    public static final int MIN_TABLE_CAPACITY = 8;

    private ForL0Options() {
        // Utility class, no instantiation
    }

    /**
     * Validates the table capacity value.
     * @param capacity the capacity to validate
     * @return true if valid (power of 2 and >= 8)
     */
    public static boolean isValidTableCapacity(int capacity) {
        return capacity >= MIN_TABLE_CAPACITY && (capacity & (capacity - 1)) == 0;
    }
}
