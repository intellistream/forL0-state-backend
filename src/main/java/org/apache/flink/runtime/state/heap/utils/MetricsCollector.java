// filepath: /Users/jinyunyang/IdeaProjects/forL0-state-backend/src/main/java/org/apache/flink/runtime/state/heap/utils/MetricsCollector.java
package org.apache.flink.runtime.state.heap.utils;

/**
 * 轻量级指标聚合器，默认常量写入开销，可按需启用/读取。
 * 为后续可插拔指标上报预留接口，不改变现有行为。
 */
public class MetricsCollector {
    private volatile boolean enabled;

    private long totalAccesses;
    private long l0Hits;
    private long mainHits;
    private long rehashCount;
    private long arenaAllocCount;

    public MetricsCollector() {
        this(false);
    }

    public MetricsCollector(boolean enabled) {
        this.enabled = enabled;
    }

    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public void incTotalAccesses() { if (enabled) totalAccesses++; }
    public void incL0Hits() { if (enabled) l0Hits++; }
    public void incMainHits() { if (enabled) mainHits++; }
    public void incRehash() { if (enabled) rehashCount++; }
    public void incArenaAlloc() { if (enabled) arenaAllocCount++; }

    public long totalAccesses() { return totalAccesses; }
    public long l0Hits() { return l0Hits; }
    public long mainHits() { return mainHits; }
    public long rehashCount() { return rehashCount; }
    public long arenaAllocCount() { return arenaAllocCount; }
}

