package org.apache.flink.state.forl0;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ForL0 State Backend - A high-performance state backend using Swiss Tables.
 *
 * <p>This state backend uses Swiss Tables architecture (aligned with Go 1.24) for
 * efficient hash table operations. It features SWAR (SIMD Within A Register) parallel
 * matching for fast lookups and Extendible Hashing for scalability.
 *
 * <h1>Key Features</h1>
 * <ul>
 *   <li>Swiss Tables with SWAR parallel matching (8 slots compared simultaneously)</li>
 *   <li>Go 1.24 style hash bit allocation (H1 for probe, H2 for control)</li>
 *   <li>Extendible Hashing for graceful scaling</li>
 *   <li>Optional L0 Cache integration for Kunpeng CPUs</li>
 * </ul>
 *
 * <h1>State Size Considerations</h1>
 *
 * <p>Working state is kept on the TaskManager heap (or L0 Cache when available).
 * If a TaskManager executes multiple tasks concurrently, the aggregate state of
 * all tasks needs to fit into that TaskManager's memory.
 *
 * <h1>Configuration</h1>
 *
 * <p>This backend can be configured via application code or Flink configuration:
 * <ul>
 *   <li>{@code state.backend.forl0.async-snapshots} - Enable async snapshots (default: true)</li>
 * </ul>
 *
 * @see ForL0Options
 */
@PublicEvolving
public class ForL0StateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ForL0StateBackend.class);

    /** Whether to use async snapshots. */
    private final boolean asyncSnapshots;

    /** L0 Cache configuration. */
    private final boolean l0CacheEnabled;
    private final long l0CacheSize;
    private final int initialTableCapacity;
    private final int maxTableCapacity;
    private final double mainTableLoadFactor;
    private final long nativeMemoryMaxSize;
    private final boolean l0CacheStrictAllocation;
    private final long l0CacheStateSize;
    private final long l0CacheWriteBypassThreshold;
    private final boolean hotCacheMetricsEnabled;

    // -----------------------------------------------------------------------

    /**
     * Creates a new ForL0 state backend with default configuration.
     */
    public ForL0StateBackend() {
        this(true);
    }

    /**
     * Creates a new ForL0 state backend.
     *
     * @param asyncSnapshots Whether to use async snapshots.
     */
    public ForL0StateBackend(boolean asyncSnapshots) {
        this.asyncSnapshots = asyncSnapshots;
        this.l0CacheEnabled = ForL0Options.L0_CACHE_ENABLED.defaultValue();
        this.l0CacheSize = ForL0Options.L0_CACHE_SIZE.defaultValue().getBytes();
        this.initialTableCapacity = ForL0Options.INITIAL_TABLE_CAPACITY.defaultValue();
        this.maxTableCapacity = ForL0Options.MAX_TABLE_CAPACITY.defaultValue();
        this.mainTableLoadFactor = ForL0Options.MAIN_TABLE_LOAD_FACTOR.defaultValue();
        this.nativeMemoryMaxSize = ForL0Options.NATIVE_MEMORY_MAX_SIZE.defaultValue().getBytes();
        this.l0CacheStrictAllocation = ForL0Options.L0_CACHE_STRICT_ALLOCATION.defaultValue();
        this.l0CacheStateSize = ForL0Options.L0_CACHE_STATE_SIZE.defaultValue().getBytes();
        this.l0CacheWriteBypassThreshold = ForL0Options.L0_CACHE_WRITE_BYPASS_THRESHOLD.defaultValue();
        this.hotCacheMetricsEnabled = ForL0Options.HOT_CACHE_METRICS_ENABLED.defaultValue();
        LOG.info("[ForL0] ForL0StateBackend created (asyncSnapshots={})", asyncSnapshots);
    }

    private ForL0StateBackend(ForL0StateBackend original, ReadableConfig config) {
        // Configure latency tracking
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
        // Configure async snapshots
        this.asyncSnapshots = config.getOptional(ForL0Options.ASYNC_SNAPSHOTS)
                .orElse(original.asyncSnapshots);
        // Configure L0 Cache
        this.l0CacheEnabled = config.getOptional(ForL0Options.L0_CACHE_ENABLED)
                .orElse(original.l0CacheEnabled);
        int expectedEngines = config.getOptional(ForL0Options.L0_CACHE_EXPECTED_ENGINES)
                .orElse(ForL0Options.L0_CACHE_EXPECTED_ENGINES.defaultValue());
        if (expectedEngines <= 0) {
            throw new IllegalConfigurationException("ForL0 l0-cache.expected-engines must be > 0");
        }
        long configuredPerEngine = config.getOptional(ForL0Options.L0_CACHE_SIZE)
                .map(MemorySize::getBytes)
                .orElse(original.l0CacheSize);
        this.l0CacheSize = config.getOptional(ForL0Options.L0_CACHE_TOTAL_SIZE)
                .map(MemorySize::getBytes)
                .map(total -> total / expectedEngines)
                .orElse(configuredPerEngine);
        this.initialTableCapacity = config.getOptional(ForL0Options.INITIAL_TABLE_CAPACITY)
                .orElse(original.initialTableCapacity);
        this.maxTableCapacity = config.getOptional(ForL0Options.MAX_TABLE_CAPACITY)
                .orElse(original.maxTableCapacity);
        this.mainTableLoadFactor = config.getOptional(ForL0Options.MAIN_TABLE_LOAD_FACTOR)
                .orElse(original.mainTableLoadFactor);
        // The canonical key wins even when explicitly set to 0 (unlimited).
        // Only consult the deprecated alias when the canonical key is absent.
        this.nativeMemoryMaxSize = config.getOptional(ForL0Options.NATIVE_MEMORY_MAX_SIZE)
                .map(MemorySize::getBytes)
                .orElseGet(() -> config.getOptional(ForL0Options.LEGACY_L0_MEMORY_MAX_SIZE)
                        .map(MemorySize::getBytes)
                        .orElse(original.nativeMemoryMaxSize));
        this.l0CacheStrictAllocation = config.getOptional(ForL0Options.L0_CACHE_STRICT_ALLOCATION)
                .orElse(original.l0CacheStrictAllocation);
        this.l0CacheStateSize = config.getOptional(ForL0Options.L0_CACHE_STATE_SIZE)
                .map(MemorySize::getBytes)
                .orElse(original.l0CacheStateSize);
        this.l0CacheWriteBypassThreshold = config.getOptional(ForL0Options.L0_CACHE_WRITE_BYPASS_THRESHOLD)
                .orElse(original.l0CacheWriteBypassThreshold);
        this.hotCacheMetricsEnabled = config.getOptional(ForL0Options.HOT_CACHE_METRICS_ENABLED)
                .orElse(original.hotCacheMetricsEnabled);
        if (!ForL0Options.isValidTableCapacity(initialTableCapacity)) {
            throw new IllegalConfigurationException(
                    "Invalid ForL0 initial table capacity: " + initialTableCapacity
                            + ". It must be a power of 2 and at least "
                            + ForL0Options.MIN_TABLE_CAPACITY + ".");
        }
        if (!ForL0Options.isValidMaxTableCapacity(maxTableCapacity)
                || (maxTableCapacity > 0 && maxTableCapacity < initialTableCapacity)) {
            throw new IllegalConfigurationException(
                    "Invalid ForL0 max table capacity: " + maxTableCapacity
                            + ". It must be 0 or a power of 2 >= initial capacity.");
        }
        if (mainTableLoadFactor < 0.5 || mainTableLoadFactor > 0.875) {
            throw new IllegalConfigurationException(
                    "ForL0 main-table.load-factor-threshold must be in [0.5, 0.875]");
        }
        if (l0CacheEnabled && l0CacheSize < 192) {
            throw new IllegalConfigurationException(
                    "ForL0 per-engine L0 quota is too small after budget division: " + l0CacheSize);
        }
        if (l0CacheStateSize < 192 || l0CacheWriteBypassThreshold < 0) {
            throw new IllegalConfigurationException(
                    "Invalid ForL0 HotCache state-size or write-bypass-threshold");
        }
    }

    // -----------------------------------------------------------------------
    //  Configuration
    // -----------------------------------------------------------------------

    @Override
    public ForL0StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new ForL0StateBackend(this, config);
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        // We never share any files, all snapshots are full
        return true;
    }

    @Override
    public boolean supportsSavepointFormat(SavepointFormatType formatType) {
        return true;
    }

    // -----------------------------------------------------------------------
    //  State Backend Creation
    // -----------------------------------------------------------------------

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {

        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(
                        parameters.getKeyGroupRange(),
                        parameters.getNumberOfKeyGroups(),
                        128);

        LatencyTrackingStateConfig latencyTrackingStateConfig =
                latencyTrackingConfigBuilder.setMetricGroup(parameters.getMetricGroup()).build();

        try {
            return new ForL0KeyedStateBackendBuilder<>(
                    parameters.getKvStateRegistry(),
                    parameters.getKeySerializer(),
                    parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                    parameters.getNumberOfKeyGroups(),
                    parameters.getKeyGroupRange(),
                    parameters.getEnv().getExecutionConfig(),
                    parameters.getTtlTimeProvider(),
                    latencyTrackingStateConfig,
                    parameters.getStateHandles(),
                    getCompressionDecorator(parameters.getEnv().getExecutionConfig()),
                    priorityQueueSetFactory,
                    asyncSnapshots,
                    l0CacheEnabled,
                    l0CacheSize,
                    initialTableCapacity,
                    maxTableCapacity,
                    mainTableLoadFactor,
                    nativeMemoryMaxSize,
                    l0CacheStrictAllocation,
                    l0CacheStateSize,
                    l0CacheWriteBypassThreshold,
                    hotCacheMetricsEnabled,
                    parameters.getCancelStreamRegistry(),
                    parameters.getMetricGroup())
                    .build();
        } catch (BackendBuildingException e) {
            throw new IOException("Failed to build ForL0KeyedStateBackend", e);
        }
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws BackendBuildingException {

        // Delegate operator state to Flink's default implementation
        return new DefaultOperatorStateBackendBuilder(
                parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
                parameters.getEnv().getExecutionConfig(),
                true,
                parameters.getStateHandles(),
                parameters.getCancelStreamRegistry())
                .build();
    }

    // -----------------------------------------------------------------------
    //  Utilities
    // -----------------------------------------------------------------------

    /**
     * Returns whether async snapshots are enabled.
     */
    public boolean isAsyncSnapshots() {
        return asyncSnapshots;
    }

    @Override
    public String toString() {
        return "ForL0StateBackend{asyncSnapshots=" + asyncSnapshots + "}";
    }
}
