package org.apache.flink.runtime.state.heap.entrystore;

/**
 * Statistics for EntryStore and its sub-pools.
 * Provides comprehensive metrics for monitoring memory usage and allocation efficiency.
 */
public class EntryStoreStats {
    
    // ========== KeyNsPool Statistics ==========
    
    /** Total segments allocated by KeyNsPool */
    private final int keyNsSegmentCount;
    
    /** Active segments (non-empty) in KeyNsPool */
    private final int keyNsActiveSegments;
    
    /** Total bytes allocated in KeyNsPool */
    private final long keyNsTotalBytes;
    
    /** Active entries in KeyNsPool */
    private final int keyNsActiveEntries;
    
    // ========== ValuePool Statistics ==========
    
    /** Total runs allocated by ValuePool */
    private final int valueRunCount;
    
    /** Active runs (non-empty) in ValuePool */
    private final int valueActiveRuns;
    
    /** Total bytes allocated in ValuePool */
    private final long valueTotalBytes;
    
    /** Active values in ValuePool */
    private final int valueActiveCount;
    
    /** Large objects count */
    private final int largeObjectCount;
    
    /** Large objects total bytes */
    private final long largeObjectBytes;
    
    // ========== Overall Statistics ==========
    
    /** Total active entries (keys) */
    private final int totalActiveEntries;
    
    /** Total system memory used */
    private final long totalSystemMemory;
    
    /** Memory efficiency percentage */
    private final double memoryEfficiency;
    
    /**
     * Creates statistics with all metrics.
     */
    public EntryStoreStats(
            int keyNsSegmentCount,
            int keyNsActiveSegments,
            long keyNsTotalBytes,
            int keyNsActiveEntries,
            int valueRunCount,
            int valueActiveRuns,
            long valueTotalBytes,
            int valueActiveCount,
            int largeObjectCount,
            long largeObjectBytes) {
        
        this.keyNsSegmentCount = keyNsSegmentCount;
        this.keyNsActiveSegments = keyNsActiveSegments;
        this.keyNsTotalBytes = keyNsTotalBytes;
        this.keyNsActiveEntries = keyNsActiveEntries;
        this.valueRunCount = valueRunCount;
        this.valueActiveRuns = valueActiveRuns;
        this.valueTotalBytes = valueTotalBytes;
        this.valueActiveCount = valueActiveCount;
        this.largeObjectCount = largeObjectCount;
        this.largeObjectBytes = largeObjectBytes;
        
        // Compute derived metrics
        this.totalActiveEntries = keyNsActiveEntries;
        this.totalSystemMemory = keyNsTotalBytes + valueTotalBytes + largeObjectBytes;
        
        if (totalSystemMemory > 0) {
            // Rough efficiency estimate: active data / total allocated
            long activeData = keyNsTotalBytes + valueTotalBytes; // Approximate
            this.memoryEfficiency = (double) activeData / totalSystemMemory * 100.0;
        } else {
            this.memoryEfficiency = 0.0;
        }
    }
    
    // ========== Getters ==========
    
    public int getKeyNsSegmentCount() {
        return keyNsSegmentCount;
    }
    
    public int getKeyNsActiveSegments() {
        return keyNsActiveSegments;
    }
    
    public long getKeyNsTotalBytes() {
        return keyNsTotalBytes;
    }
    
    public int getKeyNsActiveEntries() {
        return keyNsActiveEntries;
    }
    
    public int getValueRunCount() {
        return valueRunCount;
    }
    
    public int getValueActiveRuns() {
        return valueActiveRuns;
    }
    
    public long getValueTotalBytes() {
        return valueTotalBytes;
    }
    
    public int getValueActiveCount() {
        return valueActiveCount;
    }
    
    public int getLargeObjectCount() {
        return largeObjectCount;
    }
    
    public long getLargeObjectBytes() {
        return largeObjectBytes;
    }
    
    public int getTotalActiveEntries() {
        return totalActiveEntries;
    }
    
    public long getTotalSystemMemory() {
        return totalSystemMemory;
    }
    
    public double getMemoryEfficiency() {
        return memoryEfficiency;
    }
    
    @Override
    public String toString() {
        return String.format(
            "EntryStoreStats{" +
            "keyNs[segments=%d/%d, bytes=%d, entries=%d], " +
            "value[runs=%d/%d, bytes=%d, values=%d], " +
            "large[count=%d, bytes=%d], " +
            "total[entries=%d, memory=%d, efficiency=%.2f%%]}",
            keyNsActiveSegments, keyNsSegmentCount, keyNsTotalBytes, keyNsActiveEntries,
            valueActiveRuns, valueRunCount, valueTotalBytes, valueActiveCount,
            largeObjectCount, largeObjectBytes,
            totalActiveEntries, totalSystemMemory, memoryEfficiency
        );
    }
    
    // ========== Builder for convenient construction ==========
    
    /**
     * Builder for EntryStoreStats.
     */
    public static class Builder {
        private int keyNsSegmentCount;
        private int keyNsActiveSegments;
        private long keyNsTotalBytes;
        private int keyNsActiveEntries;
        private int valueRunCount;
        private int valueActiveRuns;
        private long valueTotalBytes;
        private int valueActiveCount;
        private int largeObjectCount;
        private long largeObjectBytes;
        
        public Builder keyNsSegmentCount(int count) {
            this.keyNsSegmentCount = count;
            return this;
        }
        
        public Builder keyNsActiveSegments(int count) {
            this.keyNsActiveSegments = count;
            return this;
        }
        
        public Builder keyNsTotalBytes(long bytes) {
            this.keyNsTotalBytes = bytes;
            return this;
        }
        
        public Builder keyNsActiveEntries(int count) {
            this.keyNsActiveEntries = count;
            return this;
        }
        
        public Builder valueRunCount(int count) {
            this.valueRunCount = count;
            return this;
        }
        
        public Builder valueActiveRuns(int count) {
            this.valueActiveRuns = count;
            return this;
        }
        
        public Builder valueTotalBytes(long bytes) {
            this.valueTotalBytes = bytes;
            return this;
        }
        
        public Builder valueActiveCount(int count) {
            this.valueActiveCount = count;
            return this;
        }
        
        public Builder largeObjectCount(int count) {
            this.largeObjectCount = count;
            return this;
        }
        
        public Builder largeObjectBytes(long bytes) {
            this.largeObjectBytes = bytes;
            return this;
        }
        
        public EntryStoreStats build() {
            return new EntryStoreStats(
                keyNsSegmentCount,
                keyNsActiveSegments,
                keyNsTotalBytes,
                keyNsActiveEntries,
                valueRunCount,
                valueActiveRuns,
                valueTotalBytes,
                valueActiveCount,
                largeObjectCount,
                largeObjectBytes
            );
        }
    }
    
    /**
     * Creates a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }
}
