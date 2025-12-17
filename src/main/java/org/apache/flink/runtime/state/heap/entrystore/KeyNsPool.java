package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;

/**
 * KeyNsPool: Key/Namespace storage pool with append-only allocation.
 * 
 * <p>Entry Layout:
 * <pre>
 * ┌────────┬────────┬────────┬─────────────┬──────┬─────────────────┐
 * │ hash   │ keyLen │ nsLen  │ valueHandle │ key  │   namespace     │
 * │ (4B)   │ (4B)   │ (4B)   │    (8B)     │(var) │     (var)       │
 * └────────┴────────┴────────┴─────────────┴──────┴─────────────────┘
 *           KEY_ENTRY_HEADER_SIZE = 20B
 * </pre>
 * 
 * <p>Design:
 * <ul>
 *   <li>Append-only allocation (O(1)) for new entries</li>
 *   <li>Segment-level live counting for reclamation</li>
 *   <li>Empty segments are released back to allocator</li>
 *   <li>Address encoding: (segmentIndex + 1) << 32 | (offset + 1)</li>
 * </ul>
 */
public class KeyNsPool implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(KeyNsPool.class);
    
    // ========== Allocator and Configuration ==========
    
    private final MemoryManagerAllocator allocator;
    private final int segmentSize;
    
    // ========== Segment Management ==========
    
    private MemorySegment[] segments;
    private List<MemorySegment>[] allocHandles;  // Original allocation handles for release
    private int segmentCount;  // Total segments allocated (including null slots)
    
    // ========== Current Write State ==========
    
    private int currentSegmentIndex;
    private int currentOffset;
    
    // ========== Live Counting (Segment-level reclamation) ==========
    
    private int[] segmentLiveCount;  // Live entries per segment
    private final Deque<Integer> freeSegmentIndices;  // Reusable empty segment slots
    
    // ========== Statistics ==========
    
    private long totalAllocatedBytes;
    private int activeEntries;
    private boolean closed;
    
    // ========== Constructors ==========
    
    /**
     * Creates a KeyNsPool with the allocator's page size as segment size.
     */
    public KeyNsPool(MemoryManagerAllocator allocator) {
        this(allocator, 0);
    }
    
    /**
     * Creates a KeyNsPool with optional pre-allocation.
     * 
     * @param allocator the memory allocator
     * @param initialSizeBytes initial memory to pre-allocate (0 for single segment)
     */
    @SuppressWarnings("unchecked")
    public KeyNsPool(MemoryManagerAllocator allocator, long initialSizeBytes) {
        this.allocator = allocator;
        this.segmentSize = allocator.getPageSize();
        
        this.segments = new MemorySegment[INITIAL_SEGMENT_CAPACITY];
        this.allocHandles = new List[INITIAL_SEGMENT_CAPACITY];
        this.segmentLiveCount = new int[INITIAL_SEGMENT_CAPACITY];
        this.segmentCount = 0;
        
        this.currentSegmentIndex = -1;
        this.currentOffset = 0;
        
        this.freeSegmentIndices = new ArrayDeque<>();
        
        this.totalAllocatedBytes = 0;
        this.activeEntries = 0;
        this.closed = false;
        
        // Allocate initial segment
        if (!allocateNewSegment()) {
            throw new IllegalStateException("Failed to allocate initial segment");
        }
        
        // Pre-allocate additional segments if requested
        if (initialSizeBytes > segmentSize) {
            int additionalSegments = (int) ((initialSizeBytes - segmentSize) / segmentSize);
            for (int i = 0; i < additionalSegments; i++) {
                if (!allocateNewSegment()) {
                    LOG.warn("Pre-allocation stopped after {} segments", segmentCount);
                    break;
                }
            }
            // Reset to first segment for writing
            currentSegmentIndex = 0;
            currentOffset = 0;
        }
    }
    
    // ========== Core Operations ==========
    
    /**
     * Allocates a new entry and returns its address.
     * 
     * @param hash the pre-computed hash value
     * @param key the key bytes
     * @param keyLen the key length
     * @param ns the namespace bytes
     * @param nsLen the namespace length
     * @param valueHandle the handle to the value in ValuePool
     * @return entry address (segmentIndex+1 << 32 | offset+1), or 0 on failure
     */
    public long allocate(int hash, byte[] key, int keyLen, byte[] ns, int nsLen, long valueHandle) {
        if (closed) {
            return NULL_HANDLE;
        }
        
        // Validate inputs
        if (key == null || keyLen < 0 || keyLen > key.length || keyLen > MAX_KEY_SIZE) {
            return NULL_HANDLE;
        }
        if (ns == null || nsLen < 0 || nsLen > ns.length || nsLen > MAX_NAMESPACE_SIZE) {
            return NULL_HANDLE;
        }
        
        // Calculate entry size (8-byte aligned)
        int entrySize = align8(KEY_ENTRY_HEADER_SIZE + keyLen + nsLen);
        
        // Check if entry is too large for a segment
        if (entrySize > segmentSize) {
            LOG.warn("Entry too large: {} bytes exceeds segment size {}", entrySize, segmentSize);
            return NULL_HANDLE;
        }
        
        // Check if current segment has enough space
        if (currentOffset + entrySize > segmentSize) {
            if (!switchToNextSegment()) {
                return NULL_HANDLE;
            }
        }
        
        // Append-only write (O(1) hot path)
        int segIdx = currentSegmentIndex;
        int offset = currentOffset;
        MemorySegment seg = segments[segIdx];
        
        // Write header
        seg.putInt(offset + KEY_HASH_OFFSET, hash);
        seg.putInt(offset + KEY_LEN_OFFSET, keyLen);
        seg.putInt(offset + NS_LEN_OFFSET, nsLen);
        seg.putLong(offset + VALUE_HANDLE_OFFSET, valueHandle);
        
        // Write key and namespace data
        seg.put(offset + KEY_DATA_OFFSET, key, 0, keyLen);
        seg.put(offset + KEY_DATA_OFFSET + keyLen, ns, 0, nsLen);
        
        // Update state
        currentOffset += entrySize;
        segmentLiveCount[segIdx]++;
        activeEntries++;
        totalAllocatedBytes += entrySize;
        
        return encodeKeyNsAddress(segIdx, offset);
    }
    
    /**
     * Releases an entry by decrementing live count.
     * Empty segments may be released back to allocator.
     * 
     * @param address the entry address
     */
    public void free(long address) {
        if (address == NULL_HANDLE) {
            return;
        }
        
        int segIdx = decodeSegmentIndex(address);
        if (segIdx < 0 || segIdx >= segmentCount || segments[segIdx] == null) {
            return;
        }
        
        segmentLiveCount[segIdx]--;
        activeEntries--;
        
        // Check if segment is now empty
        if (segmentLiveCount[segIdx] == 0) {
            releaseSegment(segIdx);
        }
    }
    
    /**
     * Updates the valueHandle for an entry (in-place update, address unchanged).
     * 
     * @param address the entry address
     * @param newValueHandle the new value handle
     */
    public void updateValueHandle(long address, long newValueHandle) {
        if (address == NULL_HANDLE) {
            return;
        }
        
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        
        if (segIdx < 0 || segIdx >= segmentCount || segments[segIdx] == null) {
            return;
        }
        
        segments[segIdx].putLong(offset + VALUE_HANDLE_OFFSET, newValueHandle);
    }
    
    // ========== Read Operations ==========
    
    /**
     * Gets the hash value of an entry.
     */
    public int getHash(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        return segments[segIdx].getInt(offset + KEY_HASH_OFFSET);
    }
    
    /**
     * Gets the key bytes of an entry.
     */
    public byte[] getKeyBytes(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        MemorySegment seg = segments[segIdx];
        
        int keyLen = seg.getInt(offset + KEY_LEN_OFFSET);
        if (keyLen <= 0) {
            return new byte[0];
        }
        if (keyLen > MAX_KEY_SIZE) {
            return null;
        }
        
        byte[] keyBytes = new byte[keyLen];
        seg.get(offset + KEY_DATA_OFFSET, keyBytes);
        return keyBytes;
    }
    
    /**
     * Gets the namespace bytes of an entry.
     */
    public byte[] getNamespaceBytes(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        MemorySegment seg = segments[segIdx];
        
        int keyLen = seg.getInt(offset + KEY_LEN_OFFSET);
        int nsLen = seg.getInt(offset + NS_LEN_OFFSET);
        
        if (nsLen <= 0) {
            return new byte[0];
        }
        if (nsLen > MAX_NAMESPACE_SIZE) {
            return null;
        }
        
        byte[] nsBytes = new byte[nsLen];
        seg.get(offset + KEY_DATA_OFFSET + keyLen, nsBytes);
        return nsBytes;
    }
    
    /**
     * Gets the value handle of an entry.
     */
    public long getValueHandle(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        return segments[segIdx].getLong(offset + VALUE_HANDLE_OFFSET);
    }
    
    // ========== Zero-Copy Slice Access ==========
    
    /**
     * Gets a zero-copy slice for the key.
     */
    public MemorySegmentSlice getKeySlice(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        MemorySegment seg = segments[segIdx];
        
        int keyLen = seg.getInt(offset + KEY_LEN_OFFSET);
        if (keyLen < 0 || keyLen > MAX_KEY_SIZE) {
            return null;
        }
        
        return new MemorySegmentSlice(seg, offset + KEY_DATA_OFFSET, keyLen);
    }
    
    /**
     * Gets a zero-copy slice for the namespace.
     */
    public MemorySegmentSlice getNamespaceSlice(long address) {
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        MemorySegment seg = segments[segIdx];
        
        int keyLen = seg.getInt(offset + KEY_LEN_OFFSET);
        int nsLen = seg.getInt(offset + NS_LEN_OFFSET);
        
        if (nsLen < 0 || nsLen > MAX_NAMESPACE_SIZE) {
            return null;
        }
        
        return new MemorySegmentSlice(seg, offset + KEY_DATA_OFFSET + keyLen, nsLen);
    }
    
    // ========== Key Matching ==========
    
    /**
     * Checks if the key and namespace match the entry at the given address.
     */
    public boolean matchesKey(long address, byte[] key, int keyLen, byte[] ns, int nsLen) {
        if (address == NULL_HANDLE || key == null || ns == null) {
            return false;
        }
        
        int segIdx = decodeSegmentIndex(address);
        int offset = decodeOffset(address);
        
        if (segIdx < 0 || segIdx >= segmentCount || segments[segIdx] == null) {
            return false;
        }
        
        MemorySegment seg = segments[segIdx];
        
        // Compare lengths first
        int storedKeyLen = seg.getInt(offset + KEY_LEN_OFFSET);
        int storedNsLen = seg.getInt(offset + NS_LEN_OFFSET);
        
        if (storedKeyLen != keyLen || storedNsLen != nsLen) {
            return false;
        }
        
        // Compare key bytes
        int dataOffset = offset + KEY_DATA_OFFSET;
        if (!equalsSegmentBytes(seg, dataOffset, key, keyLen)) {
            return false;
        }
        
        // Compare namespace bytes
        dataOffset += storedKeyLen;
        return equalsSegmentBytes(seg, dataOffset, ns, nsLen);
    }
    
    /**
     * Efficient byte array comparison with MemorySegment.
     * Compares 8 bytes at a time for better performance.
     */
    private static boolean equalsSegmentBytes(MemorySegment seg, int segOffset, byte[] arr, int len) {
        int i = 0;
        
        // Compare 8 bytes at a time
        while (i + 8 <= len) {
            long v1 = seg.getLong(segOffset + i);
            long v2 = (arr[i] & 0xFFL)
                    | ((arr[i + 1] & 0xFFL) << 8)
                    | ((arr[i + 2] & 0xFFL) << 16)
                    | ((arr[i + 3] & 0xFFL) << 24)
                    | ((arr[i + 4] & 0xFFL) << 32)
                    | ((arr[i + 5] & 0xFFL) << 40)
                    | ((arr[i + 6] & 0xFFL) << 48)
                    | ((arr[i + 7] & 0xFFL) << 56);
            if (v1 != v2) {
                return false;
            }
            i += 8;
        }
        
        // Compare 4 bytes
        if (i + 4 <= len) {
            int v1 = seg.getInt(segOffset + i);
            int v2 = (arr[i] & 0xFF)
                    | ((arr[i + 1] & 0xFF) << 8)
                    | ((arr[i + 2] & 0xFF) << 16)
                    | ((arr[i + 3] & 0xFF) << 24);
            if (v1 != v2) {
                return false;
            }
            i += 4;
        }
        
        // Compare 2 bytes
        if (i + 2 <= len) {
            short v1 = seg.getShort(segOffset + i);
            short v2 = (short) ((arr[i] & 0xFF) | ((arr[i + 1] & 0xFF) << 8));
            if (v1 != v2) {
                return false;
            }
            i += 2;
        }
        
        // Compare remaining byte
        if (i < len) {
            return seg.get(segOffset + i) == arr[i];
        }
        
        return true;
    }
    
    // ========== Segment Management ==========
    
    /**
     * Allocates a new segment.
     * 
     * @return true if successful
     */
    private boolean allocateNewSegment() {
        if (closed) {
            return false;
        }
        
        try {
            List<MemorySegment> alloc = allocator.allocate(segmentSize);
            if (alloc.isEmpty()) {
                return false;
            }
            
            int index;
            if (!freeSegmentIndices.isEmpty()) {
                // Reuse a freed slot
                index = freeSegmentIndices.pollFirst();
            } else {
                // Use new slot
                index = segmentCount;
                segmentCount++;
                ensureCapacity(index);
            }
            
            segments[index] = alloc.get(0);
            allocHandles[index] = alloc;
            segmentLiveCount[index] = 0;
            
            currentSegmentIndex = index;
            currentOffset = 0;
            
            return true;
            
        } catch (MemoryAllocationException e) {
            LOG.warn("Failed to allocate new segment", e);
            return false;
        }
    }
    
    /**
     * Switches to the next available segment for writing.
     * For append-only allocation, we always allocate a new segment
     * since we don't track per-segment write offsets.
     */
    private boolean switchToNextSegment() {
        return allocateNewSegment();
    }
    
    /**
     * Releases an empty segment back to allocator.
     */
    private void releaseSegment(int segIdx) {
        // Don't release current write segment - just reset offset
        if (segIdx == currentSegmentIndex) {
            currentOffset = 0;
            return;
        }
        
        List<MemorySegment> handle = allocHandles[segIdx];
        if (handle != null) {
            allocator.release(handle);
            allocHandles[segIdx] = null;
            segments[segIdx] = null;
            segmentLiveCount[segIdx] = 0;
            freeSegmentIndices.addLast(segIdx);
        }
    }
    
    /**
     * Ensures arrays have capacity for the given index.
     */
    @SuppressWarnings("unchecked")
    private void ensureCapacity(int index) {
        if (index < segments.length) {
            return;
        }
        
        int newLen = segments.length;
        while (newLen <= index) {
            newLen <<= 1;
        }
        
        segments = Arrays.copyOf(segments, newLen);
        allocHandles = Arrays.copyOf(allocHandles, newLen);
        segmentLiveCount = Arrays.copyOf(segmentLiveCount, newLen);
    }
    
    // ========== Statistics ==========
    
    /**
     * Gets the total number of segments (including empty slots).
     */
    public int getSegmentCount() {
        return segmentCount;
    }
    
    /**
     * Gets the number of active (non-null) segments.
     */
    public int getActiveSegmentCount() {
        int count = 0;
        for (int i = 0; i < segmentCount; i++) {
            if (segments[i] != null) {
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
     * Gets the number of active entries.
     */
    public int getActiveEntries() {
        return activeEntries;
    }
    
    /**
     * Gets the segment size.
     */
    public int getSegmentSize() {
        return segmentSize;
    }
    
    /**
     * Releases all empty segments to reduce memory footprint.
     * 
     * @return bytes released
     */
    public long releaseEmptySegments() {
        long released = 0;
        for (int i = 0; i < segmentCount; i++) {
            if (i != currentSegmentIndex && segments[i] != null && segmentLiveCount[i] == 0) {
                releaseSegment(i);
                released += segmentSize;
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
        
        // Release all segments
        for (int i = 0; i < segmentCount; i++) {
            if (allocHandles[i] != null) {
                try {
                    allocator.release(allocHandles[i]);
                } catch (Exception e) {
                    LOG.warn("Error releasing segment {} during close", i, e);
                }
                allocHandles[i] = null;
                segments[i] = null;
            }
        }
        
        freeSegmentIndices.clear();
        segmentCount = 0;
        currentSegmentIndex = -1;
        currentOffset = 0;
        totalAllocatedBytes = 0;
        activeEntries = 0;
    }
    
    /**
     * Returns true if the pool is closed.
     */
    public boolean isClosed() {
        return closed;
    }
}
