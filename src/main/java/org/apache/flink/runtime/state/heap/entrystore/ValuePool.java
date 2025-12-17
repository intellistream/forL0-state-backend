package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;

/**
 * ValuePool: Value storage pool with size-class based allocation.
 * 
 * <p>Entry Layout:
 * <pre>
 * ┌────────┬──────────────────────────────────────────────────────┐
 * │ valLen │                      value                           │
 * │ (4B)   │                      (var)                           │
 * └────────┴──────────────────────────────────────────────────────┘
 *           VALUE_ENTRY_HEADER_SIZE = 4B
 * </pre>
 * 
 * <p>Design:
 * <ul>
 *   <li>Size-class allocation for efficient memory reuse</li>
 *   <li>Bitmap-managed Runs for cache-friendly free slot lookup</li>
 *   <li>Large objects (>4KB) handled separately by LargeObjectPool</li>
 *   <li>In-place updates when new value fits in allocated slot</li>
 * </ul>
 */
public class ValuePool implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ValuePool.class);
    
    // ========== Run Configuration ==========
    
    /** Default run size (64KB per run) */
    private static final int RUN_SIZE = DEFAULT_RUN_SIZE;
    
    // ========== Allocator ==========
    
    private final MemoryManagerAllocator allocator;
    private final int runSize;
    
    // ========== Run Management (by size class) ==========
    
    /** Partial runs with free slots (per size class) */
    @SuppressWarnings("unchecked")
    private final Deque<Run>[] partialRuns = new ArrayDeque[ValueSizeClass.FIXED_SIZE_CLASS_COUNT];
    
    /** Empty runs available for reuse or release */
    @SuppressWarnings("unchecked")
    private final Deque<Run>[] emptyRuns = new ArrayDeque[ValueSizeClass.FIXED_SIZE_CLASS_COUNT];
    
    /** All runs (for statistics and cleanup) */
    private final List<Run> allRuns = new ArrayList<>();
    
    // ========== Large Object Pool ==========
    
    private final Map<Long, LargeAllocation> largeAllocations = new HashMap<>();
    private long nextLargeId = 1;
    
    // ========== Statistics ==========
    
    private long totalAllocatedBytes;
    private int activeValues;
    private int largeObjectCount;
    private long largeObjectBytes;
    private boolean closed;
    
    // ========== Constructors ==========
    
    /**
     * Creates a ValuePool with default run size.
     */
    public ValuePool(MemoryManagerAllocator allocator) {
        this(allocator, 0);
    }
    
    /**
     * Creates a ValuePool with optional pre-allocation.
     * 
     * @param allocator the memory allocator
     * @param initialSizeBytes initial memory to pre-allocate (0 for no pre-allocation)
     */
    public ValuePool(MemoryManagerAllocator allocator, long initialSizeBytes) {
        this.allocator = allocator;
        this.runSize = Math.max(allocator.getPageSize(), RUN_SIZE);
        
        // Initialize per-size-class queues
        for (int i = 0; i < ValueSizeClass.FIXED_SIZE_CLASS_COUNT; i++) {
            partialRuns[i] = new ArrayDeque<>();
            emptyRuns[i] = new ArrayDeque<>();
        }
        
        this.totalAllocatedBytes = 0;
        this.activeValues = 0;
        this.largeObjectCount = 0;
        this.largeObjectBytes = 0;
        this.closed = false;
        
        // Pre-allocate runs if requested
        if (initialSizeBytes > 0) {
            int runsToAllocate = (int) (initialSizeBytes / runSize);
            for (int i = 0; i < runsToAllocate; i++) {
                // Distribute pre-allocation across size classes
                int scIdx = i % ValueSizeClass.FIXED_SIZE_CLASS_COUNT;
                ValueSizeClass sc = ValueSizeClass.byOrdinal(scIdx);
                if (sc != null && sc.isFixedSlot()) {
                    try {
                        Run run = allocateNewRun(sc);
                        if (run != null) {
                            emptyRuns[scIdx].addLast(run);
                        }
                    } catch (Exception e) {
                        LOG.warn("Pre-allocation stopped after {} runs", i);
                        break;
                    }
                }
            }
        }
    }
    
    // ========== Core Operations ==========
    
    /**
     * Allocates space for a value.
     * 
     * @param valueLen the length of the value in bytes
     * @return value handle, or NULL_HANDLE on failure
     */
    public long allocate(int valueLen) {
        if (closed) {
            return NULL_HANDLE;
        }
        
        if (valueLen < 0 || valueLen > MAX_VALUE_SIZE) {
            return NULL_HANDLE;
        }
        
        int totalSize = VALUE_ENTRY_HEADER_SIZE + valueLen;
        ValueSizeClass sc = ValueSizeClass.getSizeClass(totalSize);
        
        if (sc == ValueSizeClass.LARGE) {
            return allocateLarge(valueLen);
        }
        
        return allocateFromRun(sc, valueLen);
    }
    
    /**
     * Frees a value slot.
     * 
     * @param valueHandle the value handle
     */
    public void free(long valueHandle) {
        if (valueHandle == NULL_HANDLE) {
            return;
        }
        
        if (isLargeObject(valueHandle)) {
            freeLarge(valueHandle);
            return;
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        
        if (run != null) {
            run.freeSlot(slotOffset);
            activeValues--;
            
            // Move run between queues based on state
            updateRunQueues(run);
        }
    }
    
    /**
     * Writes value data to an allocated slot.
     * 
     * @param valueHandle the value handle
     * @param buffer the value data
     * @param len the value length
     */
    public void write(long valueHandle, byte[] buffer, int len) {
        if (valueHandle == NULL_HANDLE || buffer == null) {
            return;
        }
        
        if (isLargeObject(valueHandle)) {
            writeLarge(valueHandle, buffer, len);
            return;
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        
        if (run != null) {
            MemorySegment seg = run.segment;
            seg.putInt(slotOffset + VALUE_LEN_OFFSET, len);
            seg.put(slotOffset + VALUE_ENTRY_HEADER_SIZE, buffer, 0, len);
        }
    }
    
    /**
     * Reads value data from a slot.
     * 
     * @param valueHandle the value handle
     * @return the value bytes, or null on error
     */
    public byte[] read(long valueHandle) {
        if (valueHandle == NULL_HANDLE) {
            return null;
        }
        
        if (isLargeObject(valueHandle)) {
            return readLarge(valueHandle);
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        
        if (run == null) {
            return null;
        }
        
        MemorySegment seg = run.segment;
        int valueLen = seg.getInt(slotOffset + VALUE_LEN_OFFSET);
        
        if (valueLen < 0 || valueLen > MAX_VALUE_SIZE) {
            return null;
        }
        
        if (valueLen == 0) {
            return new byte[0];
        }
        
        byte[] result = new byte[valueLen];
        seg.get(slotOffset + VALUE_ENTRY_HEADER_SIZE, result);
        return result;
    }
    
    /**
     * Gets a zero-copy slice for the value.
     * 
     * @param valueHandle the value handle
     * @return the memory segment slice, or null on error
     */
    public MemorySegmentSlice getSlice(long valueHandle) {
        if (valueHandle == NULL_HANDLE) {
            return null;
        }
        
        if (isLargeObject(valueHandle)) {
            return getLargeSlice(valueHandle);
        }
        
        Run run = decodeRun(valueHandle);
        int slotOffset = decodeSlotOffset(valueHandle);
        
        if (run == null) {
            return null;
        }
        
        MemorySegment seg = run.segment;
        int valueLen = seg.getInt(slotOffset + VALUE_LEN_OFFSET);
        
        if (valueLen < 0 || valueLen > MAX_VALUE_SIZE) {
            return null;
        }
        
        return new MemorySegmentSlice(seg, slotOffset + VALUE_ENTRY_HEADER_SIZE, valueLen);
    }
    
    /**
     * Gets the allocated slot size for a value handle.
     * 
     * @param valueHandle the value handle
     * @return the slot size in bytes, or -1 on error
     */
    public int getSlotSize(long valueHandle) {
        if (valueHandle == NULL_HANDLE) {
            return -1;
        }
        
        if (isLargeObject(valueHandle)) {
            LargeAllocation alloc = largeAllocations.get(valueHandle);
            return alloc != null ? alloc.allocatedSize : -1;
        }
        
        Run run = decodeRun(valueHandle);
        return run != null ? run.sizeClass.getSlotSize() : -1;
    }
    
    /**
     * Attempts to update a value in place.
     * Returns true if the new value fits in the existing slot.
     * 
     * @param valueHandle the value handle
     * @param buffer the new value data
     * @param len the new value length
     * @return true if in-place update succeeded
     */
    public boolean updateInPlace(long valueHandle, byte[] buffer, int len) {
        if (valueHandle == NULL_HANDLE || buffer == null || len < 0) {
            return false;
        }
        
        int totalSize = VALUE_ENTRY_HEADER_SIZE + len;
        
        if (isLargeObject(valueHandle)) {
            LargeAllocation alloc = largeAllocations.get(valueHandle);
            if (alloc != null && totalSize <= alloc.allocatedSize) {
                writeLarge(valueHandle, buffer, len);
                return true;
            }
            return false;
        }
        
        Run run = decodeRun(valueHandle);
        if (run == null) {
            return false;
        }
        
        if (totalSize <= run.sizeClass.getSlotSize()) {
            int slotOffset = decodeSlotOffset(valueHandle);
            MemorySegment seg = run.segment;
            seg.putInt(slotOffset + VALUE_LEN_OFFSET, len);
            seg.put(slotOffset + VALUE_ENTRY_HEADER_SIZE, buffer, 0, len);
            return true;
        }
        
        return false;
    }
    
    // ========== Run Allocation ==========
    
    /**
     * Allocates from a run of the specified size class.
     */
    private long allocateFromRun(ValueSizeClass sc, int valueLen) {
        int scIdx = sc.ordinal();
        
        // Try partial runs first
        Run run = findPartialRun(scIdx);
        
        // Try empty runs
        if (run == null) {
            run = reuseEmptyRun(scIdx);
        }
        
        // Allocate new run (already added to allRuns inside allocateNewRun)
        if (run == null) {
            run = allocateNewRun(sc);
            if (run == null) {
                return NULL_HANDLE;
            }
        }
        
        // Allocate slot from run
        int slotOffset = run.allocateSlot();
        if (slotOffset < 0) {
            return NULL_HANDLE;
        }
        
        // Write value header
        run.segment.putInt(slotOffset + VALUE_LEN_OFFSET, valueLen);
        
        activeValues++;
        totalAllocatedBytes += sc.getSlotSize();
        
        // Update run queues
        updateRunQueues(run);
        
        return encodeValueHandle(run, slotOffset);
    }
    
    /**
     * Finds a partial run with free slots.
     */
    private Run findPartialRun(int scIdx) {
        Deque<Run> partial = partialRuns[scIdx];
        while (!partial.isEmpty()) {
            Run run = partial.peekFirst();
            if (run.hasSpace()) {
                return run;
            }
            partial.pollFirst();  // Remove full run
        }
        return null;
    }
    
    /**
     * Reuses an empty run.
     */
    private Run reuseEmptyRun(int scIdx) {
        Deque<Run> empty = emptyRuns[scIdx];
        if (!empty.isEmpty()) {
            Run run = empty.pollFirst();
            partialRuns[scIdx].addFirst(run);
            return run;
        }
        return null;
    }
    
    /**
     * Allocates a new run for the given size class.
     */
    private Run allocateNewRun(ValueSizeClass sc) {
        try {
            List<MemorySegment> alloc = allocator.allocate(runSize);
            if (alloc.isEmpty()) {
                return null;
            }
            
            int index = allRuns.size();  // Index for the new run
            Run run = new Run(index, sc, alloc.get(0), alloc, runSize);
            allRuns.add(run);
            partialRuns[sc.ordinal()].addFirst(run);
            return run;
            
        } catch (MemoryAllocationException e) {
            LOG.warn("Failed to allocate new run for size class {}", sc, e);
            return null;
        }
    }
    
    /**
     * Updates run queues based on run state.
     */
    private void updateRunQueues(Run run) {
        int scIdx = run.sizeClass.ordinal();
        
        switch (run.getState()) {
            case EMPTY:
                partialRuns[scIdx].remove(run);
                emptyRuns[scIdx].addLast(run);
                break;
            case PARTIAL:
                // Should already be in partial queue, ensure it is
                if (!partialRuns[scIdx].contains(run)) {
                    emptyRuns[scIdx].remove(run);
                    partialRuns[scIdx].addFirst(run);
                }
                break;
            case FULL:
                // Remove from partial queue
                partialRuns[scIdx].remove(run);
                break;
        }
    }
    
    // ========== Large Object Handling ==========
    
    /**
     * Allocates a large object (>4KB).
     */
    private long allocateLarge(int valueLen) {
        int totalSize = VALUE_ENTRY_HEADER_SIZE + valueLen;
        int alignedSize = alignToPage(totalSize);
        
        try {
            List<MemorySegment> alloc = allocator.allocate(alignedSize);
            if (alloc.isEmpty()) {
                return NULL_HANDLE;
            }
            
            long id = nextLargeId++;
            long handle = encodeLargeHandle(id);
            
            LargeAllocation allocation = new LargeAllocation(alloc.get(0), alloc, alignedSize, valueLen);
            largeAllocations.put(handle, allocation);
            
            // Write value length
            allocation.segment.putInt(0 + VALUE_LEN_OFFSET, valueLen);
            
            largeObjectCount++;
            largeObjectBytes += alignedSize;
            activeValues++;
            
            return handle;
            
        } catch (MemoryAllocationException e) {
            LOG.warn("Failed to allocate large object of size {}", totalSize, e);
            return NULL_HANDLE;
        }
    }
    
    /**
     * Frees a large object.
     */
    private void freeLarge(long valueHandle) {
        LargeAllocation alloc = largeAllocations.remove(valueHandle);
        if (alloc != null) {
            allocator.release(alloc.allocHandle);
            largeObjectCount--;
            largeObjectBytes -= alloc.allocatedSize;
            activeValues--;
        }
    }
    
    /**
     * Writes to a large object.
     */
    private void writeLarge(long valueHandle, byte[] buffer, int len) {
        LargeAllocation alloc = largeAllocations.get(valueHandle);
        if (alloc != null) {
            alloc.segment.putInt(VALUE_LEN_OFFSET, len);
            alloc.segment.put(VALUE_ENTRY_HEADER_SIZE, buffer, 0, len);
            alloc.usedSize = len;
        }
    }
    
    /**
     * Reads from a large object.
     */
    private byte[] readLarge(long valueHandle) {
        LargeAllocation alloc = largeAllocations.get(valueHandle);
        if (alloc == null) {
            return null;
        }
        
        int len = alloc.segment.getInt(VALUE_LEN_OFFSET);
        if (len < 0 || len > MAX_VALUE_SIZE) {
            return null;
        }
        
        if (len == 0) {
            return new byte[0];
        }
        
        byte[] result = new byte[len];
        alloc.segment.get(VALUE_ENTRY_HEADER_SIZE, result);
        return result;
    }
    
    /**
     * Gets slice for a large object.
     */
    private MemorySegmentSlice getLargeSlice(long valueHandle) {
        LargeAllocation alloc = largeAllocations.get(valueHandle);
        if (alloc == null) {
            return null;
        }
        
        int len = alloc.segment.getInt(VALUE_LEN_OFFSET);
        if (len < 0 || len > MAX_VALUE_SIZE) {
            return null;
        }
        
        return new MemorySegmentSlice(alloc.segment, VALUE_ENTRY_HEADER_SIZE, len);
    }
    
    // ========== Handle Encoding/Decoding ==========
    
    // Value handle format for runs:
    // [pool_type(8b)][run_index(24b)][slot_offset(32b)]
    // Where pool_type = POOL_TYPE_VALUE (0x01)
    
    /**
     * Encodes a value handle from run and slot offset.
     * O(1) operation using Run.index field.
     */
    private long encodeValueHandle(Run run, int slotOffset) {
        return ((long) POOL_TYPE_VALUE << 56) | ((long) (run.index + 1) << 32) | (slotOffset + 1);
    }
    
    private Run decodeRun(long handle) {
        int runIndex = (int) ((handle >>> 32) & 0xFFFFFF) - 1;
        if (runIndex >= 0 && runIndex < allRuns.size()) {
            return allRuns.get(runIndex);
        }
        return null;
    }
    
    private int decodeSlotOffset(long handle) {
        return (int) (handle & 0xFFFFFFFFL) - 1;
    }
    
    private long encodeLargeHandle(long id) {
        return ((long) POOL_TYPE_LARGE << 56) | id;
    }
    
    /**
     * Checks if a handle refers to a large object.
     */
    public boolean isLargeObject(long handle) {
        return ((handle >>> 56) & 0xFF) == POOL_TYPE_LARGE;
    }
    
    // ========== Statistics ==========
    
    /**
     * Gets the total number of runs.
     */
    public int getRunCount() {
        return allRuns.size();
    }
    
    /**
     * Gets the number of active (non-empty) runs.
     */
    public int getActiveRunCount() {
        int count = 0;
        for (Run run : allRuns) {
            if (run.getState() != Run.State.EMPTY) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Gets the total allocated bytes.
     */
    public long getTotalAllocatedBytes() {
        return totalAllocatedBytes;
    }
    
    /**
     * Gets the number of active values.
     */
    public int getActiveValues() {
        return activeValues;
    }
    
    /**
     * Gets the number of large objects.
     */
    public int getLargeObjectCount() {
        return largeObjectCount;
    }
    
    /**
     * Gets the total bytes used by large objects.
     */
    public long getLargeObjectBytes() {
        return largeObjectBytes;
    }
    
    /**
     * Releases all empty runs to reduce memory footprint.
     * 
     * @return bytes released
     */
    public long releaseEmptyRuns() {
        long released = 0;
        
        for (int i = 0; i < ValueSizeClass.FIXED_SIZE_CLASS_COUNT; i++) {
            Deque<Run> empty = emptyRuns[i];
            while (!empty.isEmpty()) {
                Run run = empty.pollFirst();
                allocator.release(run.allocHandle);
                allRuns.remove(run);
                released += runSize;
            }
        }
        
        return released;
    }
    
    // ========== Lifecycle ==========
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        closed = true;
        
        // Release all runs
        for (Run run : allRuns) {
            try {
                allocator.release(run.allocHandle);
            } catch (Exception e) {
                LOG.warn("Error releasing run during close", e);
            }
        }
        allRuns.clear();
        
        // Release all large allocations
        for (LargeAllocation alloc : largeAllocations.values()) {
            try {
                allocator.release(alloc.allocHandle);
            } catch (Exception e) {
                LOG.warn("Error releasing large allocation during close", e);
            }
        }
        largeAllocations.clear();
        
        // Clear queues
        for (int i = 0; i < ValueSizeClass.FIXED_SIZE_CLASS_COUNT; i++) {
            partialRuns[i].clear();
            emptyRuns[i].clear();
        }
        
        totalAllocatedBytes = 0;
        activeValues = 0;
        largeObjectCount = 0;
        largeObjectBytes = 0;
    }
    
    /**
     * Returns true if the pool is closed.
     */
    public boolean isClosed() {
        return closed;
    }
    
    // ========== Inner Classes ==========
    
    /**
     * Run: A memory region containing fixed-size slots managed by bitmap.
     */
    static class Run {
        
        enum State { EMPTY, PARTIAL, FULL }
        
        /** Index in allRuns list for O(1) handle encoding */
        final int index;
        
        final ValueSizeClass sizeClass;
        final int slotSize;
        final int slotCount;
        
        final MemorySegment segment;
        final List<MemorySegment> allocHandle;
        final int runSize;
        
        // Bitmap (1 bit per slot, 1=used, 0=free)
        final long[] bitmap;
        
        int usedSlots;
        
        Run(int index, ValueSizeClass sizeClass, MemorySegment segment, List<MemorySegment> allocHandle, int runSize) {
            this.index = index;
            this.sizeClass = sizeClass;
            this.slotSize = sizeClass.getSlotSize();
            this.slotCount = runSize / slotSize;
            
            this.segment = segment;
            this.allocHandle = allocHandle;
            this.runSize = runSize;
            
            // Calculate bitmap size (64 bits per long)
            int bitmapLongs = (slotCount + 63) / 64;
            this.bitmap = new long[bitmapLongs];
            
            this.usedSlots = 0;
        }
        
        /**
         * Allocates a slot using bitmap.
         * Uses Long.numberOfTrailingZeros for fast free bit lookup.
         * 
         * @return slot offset, or -1 if no space
         */
        int allocateSlot() {
            if (usedSlots >= slotCount) {
                return -1;
            }
            
            // Find first free slot using bitmap
            for (int i = 0; i < bitmap.length; i++) {
                long word = bitmap[i];
                if (word != -1L) {  // Has free bits
                    // Find first zero bit
                    int bitIndex = Long.numberOfTrailingZeros(~word);
                    int slotIndex = i * 64 + bitIndex;
                    
                    if (slotIndex >= slotCount) {
                        break;  // Beyond valid slots
                    }
                    
                    // Mark slot as used
                    bitmap[i] |= (1L << bitIndex);
                    usedSlots++;
                    
                    return slotIndex * slotSize;
                }
            }
            
            return -1;
        }
        
        /**
         * Frees a slot.
         * 
         * @param slotOffset the slot offset
         */
        void freeSlot(int slotOffset) {
            int slotIndex = slotOffset / slotSize;
            int wordIndex = slotIndex / 64;
            int bitIndex = slotIndex % 64;
            
            if (wordIndex < bitmap.length) {
                bitmap[wordIndex] &= ~(1L << bitIndex);
                usedSlots--;
            }
        }
        
        /**
         * Returns true if this run has free slots.
         */
        boolean hasSpace() {
            return usedSlots < slotCount;
        }
        
        /**
         * Gets the current state of the run.
         */
        State getState() {
            if (usedSlots == 0) {
                return State.EMPTY;
            } else if (usedSlots >= slotCount) {
                return State.FULL;
            } else {
                return State.PARTIAL;
            }
        }
    }
    
    /**
     * Large allocation tracking.
     */
    private static class LargeAllocation {
        final MemorySegment segment;
        final List<MemorySegment> allocHandle;
        final int allocatedSize;
        int usedSize;
        
        LargeAllocation(MemorySegment segment, List<MemorySegment> allocHandle, int allocatedSize, int usedSize) {
            this.segment = segment;
            this.allocHandle = allocHandle;
            this.allocatedSize = allocatedSize;
            this.usedSize = usedSize;
        }
    }
}
