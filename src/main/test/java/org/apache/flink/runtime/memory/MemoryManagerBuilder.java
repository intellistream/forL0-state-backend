package org.apache.flink.runtime.memory;

import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;

/** Builder class for {@link MemoryManager}. */
public class MemoryManagerBuilder {
    private static final long DEFAULT_MEMORY_SIZE = 32L * DEFAULT_PAGE_SIZE;

    private long memorySize = DEFAULT_MEMORY_SIZE;
    private int pageSize = DEFAULT_PAGE_SIZE;

    private MemoryManagerBuilder() {}

    public MemoryManagerBuilder setMemorySize(long memorySize) {
        this.memorySize = memorySize;
        return this;
    }

    public MemoryManagerBuilder setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public MemoryManager build() {
        return new MemoryManager(memorySize, pageSize);
    }

    public static MemoryManagerBuilder newBuilder() {
        return new MemoryManagerBuilder();
    }
}