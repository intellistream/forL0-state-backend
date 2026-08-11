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
                    .defaultValue(0)
                    .withDescription("Maximum capacity of each SwissTable. 0 means unlimited; otherwise it must be a power of 2.");

    public static final ConfigOption<Double> MAIN_TABLE_LOAD_FACTOR =
            ConfigOptions.key("state.backend.forl0.main-table.load-factor-threshold")
                    .doubleType()
                    .defaultValue(0.875)
                    .withDescription("Maximum SwissTable load factor before rehash/growth [0.5, 0.875].");

    public static final ConfigOption<MemorySize> NATIVE_MEMORY_MAX_SIZE =
            ConfigOptions.key("state.backend.forl0.native-memory.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(0))
                    .withDescription("Per-StateEngine SwissTable native-memory limit. 0 means unlimited.");

    public static final ConfigOption<MemorySize> LEGACY_L0_MEMORY_MAX_SIZE =
            ConfigOptions.key("state.backend.forl0.l0-memory.max-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Deprecated alias of native-memory.max-size retained for old benchmark configs.");

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
                    .defaultValue(MemorySize.ofMebiBytes(20))
                    .withDescription("Size of L0 Cache memory pool. Examples: 20mb, 64mb.");

    public static final ConfigOption<MemorySize> L0_CACHE_TOTAL_SIZE =
            ConfigOptions.key("state.backend.forl0.l0-cache.total-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Host/device-wide L0 budget divided by l0-cache.expected-engines.");

    public static final ConfigOption<Integer> L0_CACHE_EXPECTED_ENGINES =
            ConfigOptions.key("state.backend.forl0.l0-cache.expected-engines")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Expected number of concurrent keyed StateEngines sharing the L0 device.");

    public static final ConfigOption<Boolean> L0_CACHE_STRICT_ALLOCATION =
            ConfigOptions.key("state.backend.forl0.l0-cache.strict-allocation")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Fail backend construction instead of silently shrinking or disabling an L0 quota.");

    public static final ConfigOption<MemorySize> L0_CACHE_STATE_SIZE =
            ConfigOptions.key("state.backend.forl0.l0-cache.state-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("Requested L0 quota for each eligible scalar ValueState.");

    public static final ConfigOption<Long> L0_CACHE_WRITE_BYPASS_THRESHOLD =
            ConfigOptions.key("state.backend.forl0.l0-cache.write-bypass-threshold")
                    .longType()
                    .defaultValue(1L << 20)
                    .withDescription("Consecutive cache writes without a lookup before write-only admission bypass activates; 0 disables bypass.");

    public static final ConfigOption<Boolean> HOT_CACHE_METRICS_ENABLED =
            ConfigOptions.key("forL0.metricsCollector.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to register ForL0 HotCache gauges. Keep disabled for performance runs.");

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

    public static boolean isValidMaxTableCapacity(int capacity) {
        return capacity == 0 || isValidTableCapacity(capacity);
    }
}
