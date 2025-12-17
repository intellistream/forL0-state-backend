package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration options for ForL0StateBackend.
 *
 * <p>These options can be set in Flink's config.yaml or programmatically.
 *
 * <p>Example config.yaml:
 * <pre>
 * state.backend: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
 * state.backend.forl0.l0-cache.enabled: true
 * state.backend.forl0.l0-cache.size: 12
 * state.backend.forl0.l0-cache.replacement-policy: CLOCK
 * state.backend.forl0.l0-memory.max-size: 256mb
 * state.backend.forl0.main-table.initial-size: 10
 * state.backend.forl0.main-table.load-factor-threshold: 1.5
 * </pre>
 */
public class ForL0StateBackendOptions {

    // ========== Configuration Key Prefix ==========
    
    public static final String PREFIX = "state.backend.forl0.";

    // ========== L0 Cache Configuration ==========

    /**
     * Whether L0 cache (hot key cache) is enabled.
     * When enabled, frequently accessed keys are cached in L0 memory for faster access.
     */
    public static final ConfigOption<Boolean> L0_CACHE_ENABLED =
            ConfigOptions.key(PREFIX + "l0-cache.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text("Whether L0 cache (hot key cache) is enabled. ")
                                    .text("When enabled, frequently accessed keys are cached in L0 memory for faster access.")
                                    .build());

    /**
     * Size of each L0Table as a power of 2 (bucket count).
     * Actual bucket count = 2^n, each bucket is 64 bytes with 4 slots.
     * Example: size=10 means 1024 buckets = 64KB per L0Table.
     */
    public static final ConfigOption<Integer> L0_CACHE_SIZE =
            ConfigOptions.key(PREFIX + "l0-cache.size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            Description.builder()
                                    .text("Size of each L0Table as a power of 2 (bucket count). ")
                                    .text("Actual bucket count = 2^n, each bucket is 64 bytes with 4 slots. ")
                                    .text("Example: size=10 means 1024 buckets = 64KB per L0Table. ")
                                    .text("Valid range: 1-20.")
                                    .build());

    /**
     * Replacement policy for L0 cache eviction.
     * Supported values: CLOCK, LRU, LFU, TINY_LFU, SAMPLED_LRU.
     */
    public static final ConfigOption<String> L0_CACHE_REPLACEMENT_POLICY =
            ConfigOptions.key(PREFIX + "l0-cache.replacement-policy")
                    .stringType()
                    .defaultValue("CLOCK")
                    .withDescription(
                            Description.builder()
                                    .text("Replacement policy for L0 cache eviction. Supported values: ")
                                    .list(
                                            text("CLOCK - Clock algorithm with 1-bit accessed flag (recommended, low overhead)"),
                                            text("LRU - Least Recently Used"),
                                            text("LFU - Least Frequently Used"),
                                            text("TINY_LFU - TinyLFU with decay mechanism"),
                                            text("SAMPLED_LRU - Random sampling + LRU (lightweight)"))
                                    .build());

    // ========== L0 Memory Pool Configuration ==========

    /**
     * Maximum total capacity of L0 memory pool shared by all L0Tables.
     * This is the global memory budget for all L0 caches in the StateBackend.
     * Set to 0 for unlimited (not recommended in production).
     * Default is 0 (unlimited).
     */
    public static final ConfigOption<MemorySize> L0_MEMORY_MAX_SIZE =
            ConfigOptions.key(PREFIX + "l0-memory.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO) // 0 means unlimited
                    .withDescription(
                            Description.builder()
                                    .text("Maximum total capacity of L0 memory pool shared by all L0Tables. ")
                                    .text("This is the global memory budget for all L0 caches in the StateBackend. ")
                                    .text("Set to 0 for unlimited (not recommended in production). ")
                                    .text("Example: 256mb, 1gb")
                                    .build());

    // ========== EntryStore Configuration ==========

    /**
     * Initial memory size to pre-allocate for EntryStore.
     * Pre-allocating memory reduces runtime malloc overhead.
     * Set to 0 to disable pre-allocation (allocate on demand).
     * Default is 0 (no pre-allocation).
     */
    public static final ConfigOption<MemorySize> ARENA_INITIAL_SIZE =
            ConfigOptions.key(PREFIX + "arena.initial-size")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO)
                    .withDescription(
                            Description.builder()
                                    .text("Initial memory size to pre-allocate for EntryStore. ")
                                    .text("Pre-allocating memory reduces runtime malloc overhead. ")
                                    .text("Set to 0 to disable pre-allocation (allocate on demand). ")
                                    .text("Example: 64mb, 128mb, 256mb")
                                    .build());

    // ========== MainTable Configuration ==========

    /**
     * Initial size of MainTable as a power of 2 (bucket count).
     * Actual bucket count = 2^n, each bucket is 64 bytes with 6 slots.
     * The table will automatically resize when load factor threshold is exceeded.
     */
    public static final ConfigOption<Integer> MAIN_TABLE_INITIAL_SIZE =
            ConfigOptions.key(PREFIX + "main-table.initial-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            Description.builder()
                                    .text("Initial size of MainTable as a power of 2 (bucket count). ")
                                    .text("Actual bucket count = 2^n, each bucket is 64 bytes with 6 slots. ")
                                    .text("Example: size=10 means 1024 initial buckets. ")
                                    .text("The table will automatically resize when load factor threshold is exceeded. ")
                                    .text("Valid range: 1-20.")
                                    .build());

    /**
     * Load factor threshold that triggers MainTable resize.
     * When (entries / buckets) exceeds this value, the table will double in size.
     */
    public static final ConfigOption<Double> MAIN_TABLE_LOAD_FACTOR_THRESHOLD =
            ConfigOptions.key(PREFIX + "main-table.load-factor-threshold")
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription(
                            Description.builder()
                                    .text("Load factor threshold that triggers MainTable resize. ")
                                    .text("When (entries / buckets) exceeds this value, the table will double in size. ")
                                    .text("Higher values reduce memory usage but may increase lookup time. ")
                                    .text("Valid range: 0.5-4.0. Recommended: 1.0-2.0.")
                                    .build());

    // ========== Utility Methods ==========

    /**
     * Validates the L0 cache size parameter.
     *
     * @param size The configured size value
     * @throws IllegalArgumentException if the value is out of valid range
     */
    public static void validateL0CacheSize(int size) {
        if (size < 1 || size > 20) {
            throw new IllegalArgumentException(
                    String.format("Invalid L0 cache size: %d. Valid range is 1-20 (2^1 to 2^20 buckets).", size));
        }
    }

    /**
     * Validates the MainTable initial size parameter.
     *
     * @param size The configured size value
     * @throws IllegalArgumentException if the value is out of valid range
     */
    public static void validateMainTableInitialSize(int size) {
        if (size < 1 || size > 20) {
            throw new IllegalArgumentException(
                    String.format("Invalid MainTable initial size: %d. Valid range is 1-20 (2^1 to 2^20 buckets).", size));
        }
    }

    /**
     * Validates the load factor threshold parameter.
     *
     * @param threshold The configured threshold value
     * @throws IllegalArgumentException if the value is out of valid range
     */
    public static void validateLoadFactorThreshold(double threshold) {
        if (threshold < 0.5 || threshold > 4.0) {
            throw new IllegalArgumentException(
                    String.format("Invalid load factor threshold: %.2f. Valid range is 0.5-4.0.", threshold));
        }
    }

    /**
     * Parses and validates the replacement policy string.
     *
     * @param policyString The configured policy string
     * @return The corresponding ReplacementPolicy enum
     * @throws IllegalArgumentException if the policy string is invalid
     */
    public static L0Table.ReplacementPolicy parseReplacementPolicy(String policyString) {
        if (policyString == null || policyString.trim().isEmpty()) {
            return L0Table.ReplacementPolicy.CLOCK;
        }
        try {
            return L0Table.ReplacementPolicy.valueOf(policyString.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid replacement policy: '%s'. Supported values: CLOCK, LRU, LFU, TINY_LFU, SAMPLED_LRU.",
                            policyString));
        }
    }

    private ForL0StateBackendOptions() {
        // Utility class, no instantiation
    }
}
