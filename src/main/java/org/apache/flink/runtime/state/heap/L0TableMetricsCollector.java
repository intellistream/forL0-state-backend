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
import java.util.ArrayList;
import java.util.List;
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
 * L0Table statistics. The samples are output to the log with a special
 * prefix (L0TABLE_METRICS) for easy parsing by benchmark scripts.
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
    
    /** List of L0Tables to collect metrics from */
    private final CopyOnWriteArrayList<L0Table> l0Tables;
    
    /** List of ForL0StateMaps to collect cache stats from */
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
        this.l0Tables = new CopyOnWriteArrayList<>();
        this.stateMaps = new CopyOnWriteArrayList<>();
        this.running = new AtomicBoolean(false);
        this.sampleCount = 0;
    }
    
    /**
     * Registers an L0Table for metrics collection.
     * Thread-safe, can be called while collector is running.
     */
    public void registerL0Table(L0Table l0Table) {
        if (l0Table != null) {
            l0Tables.add(l0Table);
            LOG.debug("[{}] Registered L0Table for metrics collection", LOG_PREFIX);
        }
    }
    
    /**
     * Registers a ForL0StateMap for cache stats collection.
     * Thread-safe, can be called while collector is running.
     */
    public void registerStateMap(ForL0StateMap<?, ?, ?> stateMap) {
        if (stateMap != null) {
            stateMaps.add(stateMap);
            LOG.debug("[{}] Registered ForL0StateMap for metrics collection", LOG_PREFIX);
        }
    }
    
    /**
     * Extracts L0Tables from registered StateTables in the backend.
     * Call this after all states have been registered.
     */
    public <K> void extractFromRegisteredStates(Map<String, StateTable<K, ?, ?>> registeredKVStates) {
        for (StateTable<K, ?, ?> stateTable : registeredKVStates.values()) {
            if (stateTable instanceof ForL0StateTable) {
                ForL0StateTable<K, ?, ?> forL0StateTable = (ForL0StateTable<K, ?, ?>) stateTable;
                extractFromStateTable(forL0StateTable);
            }
        }
        LOG.info("[{}] Extracted metrics sources: {} L0Tables, {} StateMaps", 
                LOG_PREFIX, l0Tables.size(), stateMaps.size());
    }
    
    @SuppressWarnings("unchecked")
    private <K, N, S> void extractFromStateTable(ForL0StateTable<K, N, S> stateTable) {
        // Access keyGroupedStateMaps via reflection or getter if available
        // For now, we rely on the StateTable's snapshot method structure
        try {
            // ForL0StateTable extends StateTable which has protected keyGroupedStateMaps
            java.lang.reflect.Field field = StateTable.class.getDeclaredField("keyGroupedStateMaps");
            field.setAccessible(true);
            StateMap<K, N, S>[] maps = (StateMap<K, N, S>[]) field.get(stateTable);
            
            for (StateMap<K, N, S> map : maps) {
                if (map instanceof ForL0StateMap) {
                    ForL0StateMap<K, N, S> forL0Map = (ForL0StateMap<K, N, S>) map;
                    registerStateMap(forL0Map);
                    
                    // Extract L0Table from ForL0StateMap
                    java.lang.reflect.Field l0Field = ForL0StateMap.class.getDeclaredField("l0Table");
                    l0Field.setAccessible(true);
                    L0Table l0Table = (L0Table) l0Field.get(forL0Map);
                    registerL0Table(l0Table);
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOG.warn("[{}] Failed to extract L0Tables via reflection: {}", LOG_PREFIX, e.getMessage());
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
     * Collects metrics from all registered L0Tables and logs them.
     * Called periodically by the scheduler.
     */
    private void collectAndLogMetrics() {
        if (!running.get()) {
            return;
        }
        
        long currentTimeMs = System.currentTimeMillis();
        long elapsedMs = currentTimeMs - startTimeMs;
        sampleCount++;
        
        // Collect L0Table stats
        List<L0TableSample> l0Samples = new ArrayList<>();
        for (int i = 0; i < l0Tables.size(); i++) {
            L0Table table = l0Tables.get(i);
            if (table != null) {
                try {
                    L0Table.L0TableStats stats = table.getStats();
                    l0Samples.add(new L0TableSample(i, stats));
                } catch (Exception e) {
                    // Ignore - table might be closed
                }
            }
        }
        
        // Collect cache stats from StateMaps
        List<CacheStatsSample> cacheSamples = new ArrayList<>();
        for (int i = 0; i < stateMaps.size(); i++) {
            ForL0StateMap<?, ?, ?> map = stateMaps.get(i);
            if (map != null) {
                try {
                    ForL0StateMap.CacheStats stats = map.getCacheStats();
                    cacheSamples.add(new CacheStatsSample(i, stats));
                } catch (Exception e) {
                    // Ignore - map might be closed
                }
            }
        }
        
        // Output aggregated metrics
        if (!l0Samples.isEmpty()) {
            // Aggregate L0Table stats
            long totalAccessCount = 0;
            long totalHitCount = 0;
            long totalMissCount = 0;
            long totalEvictionCount = 0;
            int totalValidSlots = 0;
            
            for (L0TableSample sample : l0Samples) {
                totalAccessCount += sample.stats.accessCount;
                totalHitCount += sample.stats.hitCount;
                totalMissCount += sample.stats.missCount;
                totalEvictionCount += sample.stats.evictionCount;
                totalValidSlots += sample.stats.validSlots;
            }
            
            double hitRate = totalAccessCount > 0 ? (double) totalHitCount / totalAccessCount : 0.0;
            
            // Output in parseable JSON format
            // Format: L0TABLE_METRICS|backendId|sampleNum|elapsedMs|JSON
            String json = String.format(
                    "{\"type\":\"l0table\",\"backend\":\"%s\",\"sample\":%d,\"elapsed_ms\":%d," +
                    "\"access_count\":%d,\"hit_count\":%d,\"miss_count\":%d,\"eviction_count\":%d," +
                    "\"valid_slots\":%d,\"hit_rate\":%.4f,\"table_count\":%d}",
                    backendId, sampleCount, elapsedMs,
                    totalAccessCount, totalHitCount, totalMissCount, totalEvictionCount,
                    totalValidSlots, hitRate, l0Samples.size());
            
            LOG.info("{}|{}", LOG_PREFIX, json);
        }
        
        // Output cache stats
        if (!cacheSamples.isEmpty()) {
            long totalAccesses = 0;
            long totalL0Hits = 0;
            long totalMainTableHits = 0;
            int totalEntries = 0;
            
            for (CacheStatsSample sample : cacheSamples) {
                totalAccesses += sample.stats.totalAccesses;
                totalL0Hits += sample.stats.l0Hits;
                totalMainTableHits += sample.stats.mainTableHits;
                totalEntries += sample.stats.totalEntries;
            }
            
            double l0HitRate = totalAccesses > 0 ? (double) totalL0Hits / totalAccesses : 0.0;
            double overallHitRate = totalAccesses > 0 
                    ? (double) (totalL0Hits + totalMainTableHits) / totalAccesses : 0.0;
            
            String json = String.format(
                    "{\"type\":\"cache\",\"backend\":\"%s\",\"sample\":%d,\"elapsed_ms\":%d," +
                    "\"total_accesses\":%d,\"l0_hits\":%d,\"main_table_hits\":%d," +
                    "\"total_entries\":%d,\"l0_hit_rate\":%.4f,\"overall_hit_rate\":%.4f,\"map_count\":%d}",
                    backendId, sampleCount, elapsedMs,
                    totalAccesses, totalL0Hits, totalMainTableHits,
                    totalEntries, l0HitRate, overallHitRate, cacheSamples.size());
            
            LOG.info("{}|{}", LOG_PREFIX, json);
        }
    }
    
    /**
     * Outputs final summary metrics.
     * Called when the collector is stopped.
     */
    private void outputFinalSummary() {
        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        
        // Collect final L0Table stats
        for (int i = 0; i < l0Tables.size(); i++) {
            L0Table table = l0Tables.get(i);
            if (table != null) {
                try {
                    L0Table.L0TableStats stats = table.getStats();
                    String json = String.format(
                            "{\"type\":\"l0table_final\",\"backend\":\"%s\",\"table_index\":%d," +
                            "\"total_elapsed_ms\":%d,\"total_samples\":%d," +
                            "\"access_count\":%d,\"hit_count\":%d,\"miss_count\":%d,\"eviction_count\":%d," +
                            "\"valid_slots\":%d,\"load_factor\":%.4f,\"hit_rate\":%.4f,\"miss_rate\":%.4f}",
                            backendId, i, elapsedMs, sampleCount,
                            stats.accessCount, stats.hitCount, stats.missCount, stats.evictionCount,
                            stats.validSlots, stats.loadFactor, stats.hitRate, stats.missRate);
                    
                    LOG.info("{}|{}", LOG_PREFIX, json);
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
        
        // Collect final cache stats
        for (int i = 0; i < stateMaps.size(); i++) {
            ForL0StateMap<?, ?, ?> map = stateMaps.get(i);
            if (map != null) {
                try {
                    ForL0StateMap.CacheStats stats = map.getCacheStats();
                    String json = String.format(
                            "{\"type\":\"cache_final\",\"backend\":\"%s\",\"map_index\":%d," +
                            "\"total_elapsed_ms\":%d,\"total_samples\":%d," +
                            "\"total_accesses\":%d,\"l0_hits\":%d,\"main_table_hits\":%d," +
                            "\"total_entries\":%d,\"l0_hit_rate\":%.4f,\"overall_hit_rate\":%.4f}",
                            backendId, i, elapsedMs, sampleCount,
                            stats.totalAccesses, stats.l0Hits, stats.mainTableHits,
                            stats.totalEntries, stats.getL0HitRate(), stats.getOverallHitRate());
                    
                    LOG.info("{}|{}", LOG_PREFIX, json);
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
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
        l0Tables.clear();
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
    
    // Helper classes for collecting samples
    
    private static class L0TableSample {
        final int index;
        final L0Table.L0TableStats stats;
        
        L0TableSample(int index, L0Table.L0TableStats stats) {
            this.index = index;
            this.stats = stats;
        }
    }
    
    private static class CacheStatsSample {
        final int index;
        final ForL0StateMap.CacheStats stats;
        
        CacheStatsSample(int index, ForL0StateMap.CacheStats stats) {
            this.index = index;
            this.stats = stats;
        }
    }
}
