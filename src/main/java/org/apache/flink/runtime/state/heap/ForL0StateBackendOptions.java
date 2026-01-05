package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;

/**
 * Configuration options for ForL0StateBackend with SwissMap architecture.
 *
 * <p>The new SwissMap-based architecture uses adaptive directory expansion,
 * requiring minimal configuration compared to the previous MainTable+L0Table design.
 *
 * <p>Example config.yaml:
 * <pre>
 * state.backend: org.apache.flink.runtime.state.heap.ForL0StateBackendFactory
 * state.backend.forl0.l0-memory.max-size: 256mb
 * </pre>
 */
public class ForL0StateBackendOptions {

    // ========== Configuration Key Prefix ==========
    
    public static final String PREFIX = "state.backend.forl0.";

    // ========== Native Memory Pool Configuration ==========

    /**
     * Maximum total capacity of native memory pool.
     * This is the global memory budget for the StateBackend.
     * Set to 0 for unlimited (default).
     */
    public static final ConfigOption<MemorySize> L0_MEMORY_MAX_SIZE =
            ConfigOptions.key(PREFIX + "l0-memory.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO) // 0 means unlimited
                    .withDescription(
                            Description.builder()
                                    .text("Maximum total capacity of native memory pool. ")
                                    .text("Set to 0 for unlimited (default). ")
                                    .text("Example: 64mb, 128mb, 256mb")
                                    .build());

    // ========== Legacy Configuration Options (Deprecated) ==========
    // These options are kept for backward compatibility but have no effect
    // in the new SwissMap architecture.

    /**
     * @deprecated No longer used in SwissMap architecture. Kept for backward compatibility.
     */
    @Deprecated
    public static final ConfigOption<Boolean> L0_CACHE_ENABLED =
            ConfigOptions.key(PREFIX + "l0-cache.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Deprecated: No effect in SwissMap architecture.");

    /**
     * @deprecated No longer used in SwissMap architecture. Kept for backward compatibility.
     */
    @Deprecated
    public static final ConfigOption<Integer> L0_CACHE_SIZE =
            ConfigOptions.key(PREFIX + "l0-cache.size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Deprecated: No effect in SwissMap architecture.");

    /**
     * @deprecated No longer used in SwissMap architecture. Kept for backward compatibility.
     */
    @Deprecated
    public static final ConfigOption<String> L0_CACHE_REPLACEMENT_POLICY =
            ConfigOptions.key(PREFIX + "l0-cache.replacement-policy")
                    .stringType()
                    .defaultValue("CLOCK")
                    .withDescription("Deprecated: No effect in SwissMap architecture.");

    /**
     * @deprecated No longer used in SwissMap architecture. Kept for backward compatibility.
     */
    @Deprecated
    public static final ConfigOption<Double> MAIN_TABLE_LOAD_FACTOR_THRESHOLD =
            ConfigOptions.key(PREFIX + "main-table.load-factor-threshold")
                    .doubleType()
                    .defaultValue(1.5)
                    .withDescription("Deprecated: No effect in SwissMap architecture.");

    private ForL0StateBackendOptions() {
        // Utility class, no instantiation
    }
}
