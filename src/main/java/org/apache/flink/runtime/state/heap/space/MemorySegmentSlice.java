// ...new file...
package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;

/**
 * Lightweight slice view of a MemorySegment for zero-copy access.
 * Used by EntryStore for efficient memory access.
 * 
 * <p>This class is designed for reuse to avoid object allocation in hot paths.
 * Use {@link #set(MemorySegment, int, int)} to update the slice in-place.
 */
public final class MemorySegmentSlice {
    public MemorySegment segment;
    public int offset;
    public int length;

    public MemorySegmentSlice(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
    }
    
    /**
     * Default constructor for reusable instances.
     */
    public MemorySegmentSlice() {
        this.segment = null;
        this.offset = 0;
        this.length = 0;
    }
    
    /**
     * Updates this slice in-place to avoid allocation.
     * 
     * @param segment the memory segment
     * @param offset the offset within the segment
     * @param length the length of the slice
     * @return this slice for chaining
     */
    public MemorySegmentSlice set(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.offset = offset;
        this.length = length;
        return this;
    }
    
    /**
     * Resets this slice to empty state.
     */
    public void clear() {
        this.segment = null;
        this.offset = 0;
        this.length = 0;
    }
    
    /**
     * @return true if this slice is valid (has a segment)
     */
    public boolean isValid() {
        return segment != null;
    }
}

