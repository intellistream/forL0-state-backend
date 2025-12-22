/*
 * [BENCHMARK_TEST] This class is for benchmark testing only.
 * Collects L0Table metrics periodically for performance analysis.
 * Should be removed or disabled in production code.
 */
package org.apache.flink.runtime.state.heap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * [BENCHMARK_TEST] Collects L0Table metrics at regular intervals.
 * 
 * This collector runs in an independent thread and periodically samples
 * L0Table statistics via ForL0StateMap.getL0Stats(). The samples are 
 * output to the log with a special prefix (L0TABLE_METRICS) for easy 
 * parsing by benchmark scripts.
 * 
 * <p>Thread Safety: The collector uses volatile/atomic counters in L0Table
 * for thread-safe reads. Flink state access is single-threaded, so the
 * writes to counters don't need synchronization.
 * 
 * <p>Performance Impact: Minimal - only reads volatile longs and outputs
 * to log at 1-second intervals. Does not affect hot path.
 */
public class L0TableMetricsCollector implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(L0TableMetricsCollector.class);
    
    /** Prefix for log output, used for parsing by benchmark scripts */
    private static final String LOG_PREFIX = "L0TABLE_METRICS";
    
    /** Default sampling interval in milliseconds */
    private static final long DEFAULT_SAMPLE_INTERVAL_MS = 1000;
    
    /** List of ForL0StateMaps to collect stats from (includes L0 stats) */
    private final CopyOnWriteArrayList<ForL0StateMap<?, ?, ?>> stateMaps;
    
    /** Scheduled executor for periodic sampling */
    @Nullable
    private ScheduledExecutorService scheduler;
    
    /** Sampling interval in milliseconds */
    private final long sampleIntervalMs;
    
    /** Flag to track if collector is running */
    private final AtomicBoolean running;
    
    /** Start timestamp for relative time calculation */
    private long startTimeMs;
    
    /** Sample sequence counter */
    private long sampleCount;
    
    /** Backend identifier for multi-backend scenarios */
    private final String backendId;

    /**
     * Creates a new metrics collector with default 1-second interval.
     * 
     * @param backendId identifier for this backend (e.g., subtask index)
     */
    public L0TableMetricsCollector(String backendId) {
        this(backendId, DEFAULT_SAMPLE_INTERVAL_MS);
    }
    
    /**
     * Creates a new metrics collector with specified interval.
     * 
     * @param backendId identifier for this backend
     * @param sampleIntervalMs sampling interval in milliseconds
     */
    public L0TableMetricsCollector(String backendId, long sampleIntervalMs) {
        this.backendId = backendId;
        this.sampleIntervalMs = sampleIntervalMs;
        this.stateMaps = new CopyOnWriteArrayList<>();
        this.running = new AtomicBoolean(false);
        this.sampleCount = 0;
    }
    
    /**
     * Registers a ForL0StateMap for stats collection.
     * Thread-safe, can be called while collector is running.
     */
    public void registerStateMap(ForL0StateMap<?, ?, ?> stateMap) {
        if (stateMap != null) {
            stateMaps.add(stateMap);
            LOG.debug("[{}] Registered ForL0StateMap for metrics collection", LOG_PREFIX);
        }
    }
    
    /**
     * Extracts ForL0StateMaps from registered StateTables in the backend.
     * Call this after all states have been registered.
     */
    public <K> void extractFromRegisteredStates(Map<String, StateTable<K, ?, ?>> registeredKVStates) {
        for (StateTable<K, ?, ?> stateTable : registeredKVStates.values()) {
            if (stateTable instanceof ForL0StateTable) {
                ForL0StateTable<K, ?, ?> forL0StateTable = (ForL0StateTable<K, ?, ?>) stateTable;
                extractFromStateTable(forL0StateTable);
            }
        }
        LOG.info("[{}] Extracted {} StateMaps for metrics collection", LOG_PREFIX, stateMaps.size());
    }
    
    @SuppressWarnings("unchecked")
    private <K, N, S> void extractFromStateTable(ForL0StateTable<K, N, S> stateTable) {
        try {
            // ForL0StateTable extends StateTable which has protected keyGroupedStateMaps
            java.lang.reflect.Field field = StateTable.class.getDeclaredField("keyGroupedStateMaps");
            field.setAccessible(true);
            StateMap<K, N, S>[] maps = (StateMap<K, N, S>[]) field.get(stateTable);
            
            for (StateMap<K, N, S> map : maps) {
                if (map instanceof ForL0StateMap) {
                    registerStateMap((ForL0StateMap<K, N, S>) map);
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOG.warn("[{}] Failed to extract StateMaps via reflection: {}", LOG_PREFIX, e.getMessage());
        }
    }
    
    /**
     * Starts the metrics collection.
     * Creates a daemon thread that samples at the configured interval.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            startTimeMs = System.currentTimeMillis();
            sampleCount = 0;
            
            scheduler = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "L0TableMetricsCollector-" + backendId);
                t.setDaemon(true);
                return t;
            });
            
            scheduler.scheduleAtFixedRate(
                    this::collectAndLogMetrics,
                    sampleIntervalMs,  // initial delay
                    sampleIntervalMs,  // period
                    TimeUnit.MILLISECONDS);
            
            LOG.info("[{}] Started metrics collection for backend {} with {}ms interval", 
                    LOG_PREFIX, backendId, sampleIntervalMs);
        }
    }
    
    /**
     * Collects metrics from all registered StateMaps and logs them.
     * Called periodically by the scheduler.
     */
    private void collectAndLogMetrics() {
        if (!running.get() || stateMaps.isEmpty()) {
            return;
        }
        
        long currentTimeMs = System.currentTimeMillis();
        long elapsedMs = currentTimeMs - startTimeMs;
        sampleCount++;
        
        // Aggregate L0Table stats from all StateMaps
        long totalAccessCount = 0;
        long totalHitCount = 0;
        long totalMissCount = 0;
        long totalEvictionCount = 0;
        int totalValidSlots = 0;
        int l0TableCount = 0;
        
        // Aggregate StateMap stats
        int totalEntries = 0;
        
        for (ForL0StateMap<?, ?, ?> map : stateMaps) {
            try {
                totalEntries += map.size();
                
                // Get L0 stats if available
                L0Table.L0TableStats l0Stats = map.getL0Stats();
                if (l0Stats != null) {
                    totalAccessCount += l0Stats.accessCount;
                    totalHitCount += l0Stats.hitCount;
                    totalMissCount += l0Stats.missCount;
                    totalEvictionCount += l0Stats.evictionCount;
                    totalValidSlots += l0Stats.validSlots;
                    l0TableCount++;
                }
            } catch (Exception e) {
                // Ignore - map might be closed
            }
        }
        
        // Output L0Table metrics if L0 is enabled
        if (l0TableCount > 0) {
            double hitRate = totalAccessCount > 0 ? (double) totalHitCount / totalAccessCount : 0.0;
            double timeSeconds = elapsedMs / 1000.0;
            
            String json = String.format(
                    "{\"type\":\"l0table\",\"backend_id\":\"%s\",\"sample\":%d,\"time_seconds\":%.3f," +
                    "\"accesses\":%d,\"hits\":%d,\"misses\":%d,\"evictions\":%d," +
                    "\"valid_slots\":%d,\"hit_rate\":%.4f,\"table_count\":%d}",
                    backendId, sampleCount, timeSeconds,
                    totalAccessCount, totalHitCount, totalMissCount, totalEvictionCount,
                    totalValidSlots, hitRate, l0TableCount);
            
            LOG.info("{}|{}", LOG_PREFIX, json);
        }
        
        // Output StateMap stats
        double stateMapTimeSeconds = elapsedMs / 1000.0;
        String stateMapJson = String.format(
                "{\"type\":\"statemap\",\"backend_id\":\"%s\",\"sample\":%d,\"time_seconds\":%.3f," +
                "\"total_entries\":%d,\"map_count\":%d}",
                backendId, sampleCount, stateMapTimeSeconds, totalEntries, stateMaps.size());
        
        LOG.info("{}|{}", LOG_PREFIX, stateMapJson);
    }
    
    /**
     * Outputs final summary metrics.
     * Called when the collector is stopped.
     */
    private void outputFinalSummary() {
        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        
        // Aggregate final stats
        long totalAccessCount = 0;
        long totalHitCount = 0;
        long totalMissCount = 0;
        long totalEvictionCount = 0;
        int totalValidSlots = 0;
        int l0TableCount = 0;
        int totalEntries = 0;
        
        for (ForL0StateMap<?, ?, ?> map : stateMaps) {
            try {
                totalEntries += map.size();
                
                L0Table.L0TableStats l0Stats = map.getL0Stats();
                if (l0Stats != null) {
                    totalAccessCount += l0Stats.accessCount;
                    totalHitCount += l0Stats.hitCount;
                    totalMissCount += l0Stats.missCount;
                    totalEvictionCount += l0Stats.evictionCount;
                    totalValidSlots += l0Stats.validSlots;
                    l0TableCount++;
                }
            } catch (Exception e) {
                // Ignore
            }
        }
        
        // Output final L0Table summary
        if (l0TableCount > 0) {
            double hitRate = totalAccessCount > 0 ? (double) totalHitCount / totalAccessCount : 0.0;
            double missRate = totalAccessCount > 0 ? (double) totalMissCount / totalAccessCount : 0.0;
            double totalTimeSeconds = elapsedMs / 1000.0;
            
            String json = String.format(
                    "{\"type\":\"l0table_final\",\"backend_id\":\"%s\",\"total_time_seconds\":%.3f,\"total_samples\":%d," +
                    "\"accesses\":%d,\"hits\":%d,\"misses\":%d,\"evictions\":%d," +
                    "\"valid_slots\":%d,\"hit_rate\":%.4f,\"miss_rate\":%.4f,\"table_count\":%d}",
                    backendId, totalTimeSeconds, sampleCount,
                    totalAccessCount, totalHitCount, totalMissCount, totalEvictionCount,
                    totalValidSlots, hitRate, missRate, l0TableCount);
            
            LOG.info("{}|{}", LOG_PREFIX, json);
        }
        
        // Output final StateMap summary
        double stateMapTimeSeconds = elapsedMs / 1000.0;
        String stateMapJson = String.format(
                "{\"type\":\"statemap_final\",\"backend_id\":\"%s\",\"total_time_seconds\":%.3f," +
                "\"total_samples\":%d,\"total_entries\":%d,\"map_count\":%d}",
                backendId, stateMapTimeSeconds, sampleCount, totalEntries, stateMaps.size());
        
        LOG.info("{}|{}", LOG_PREFIX, stateMapJson);
    }
    
    /**
     * Stops the metrics collection and outputs final summary.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            ScheduledExecutorService sched = scheduler;
            if (sched != null) {
                sched.shutdown();
                try {
                    if (!sched.awaitTermination(5, TimeUnit.SECONDS)) {
                        sched.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    sched.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            // Output final summary
            outputFinalSummary();
            
            LOG.info("[{}] Stopped metrics collection for backend {}", LOG_PREFIX, backendId);
        }
    }
    
    @Override
    public void close() {
        stop();
        stateMaps.clear();
    }
    
    /**
     * Returns whether the collector is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Returns the number of samples collected so far.
     */
    public long getSampleCount() {
        return sampleCount;
    }
}
