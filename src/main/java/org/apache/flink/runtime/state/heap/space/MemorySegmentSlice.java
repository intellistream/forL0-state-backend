// ...new file...
package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;

/**
 * Lightweight slice view of a MemorySegment for zero-copy access.
 * Used by EntryStore for efficient memory access.
 */
public final class MemorySegmentSlice {
    public final MemorySegment segment;
    public final int offset;
    public final int length;

    public MemorySegmentSlice(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
    }
}

