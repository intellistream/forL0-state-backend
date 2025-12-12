package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;
import org.apache.flink.runtime.state.heap.utils.HashFunctions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * Entry Arena manages the actual storage of key-value entries.
 * Stores serialized key/namespace/value byte arrays directly in off-heap memory.
 * Entry format: [hash(4B)][keyLen(4B)][namespaceLen(4B)][valueLen(4B)][key][namespace][value]
 * Simplified to single allocation strategy: FREE_LIST with size classes for reuse.
 * Safe implementation: uses Flink MemorySegment instead of Unsafe operations.
 */
public class EntryArena implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(EntryArena.class);

    // Entry layout constants
    private static final int HASH_OFFSET = 0;           // 4 bytes
    private static final int KEY_LEN_OFFSET = 4;        // 4 bytes
    private static final int NAMESPACE_LEN_OFFSET = 8;  // 4 bytes
    private static final int VALUE_LEN_OFFSET = 12;     // 4 bytes
    private static final int ENTRY_HEADER_SIZE = 16;    // Total header size

    // Memory alignment (8 bytes for better performance)
    private static final int ALIGNMENT = 8;
    private static final int MIN_ENTRY_SIZE = ENTRY_HEADER_SIZE + ALIGNMENT;

    // Memory segment management
    // private static final int SEGMENT_SIZE = 64 * 1024;  // 64KB per slab (replaced by dynamic page-aligned size)
    private final int segmentSize; // use allocator.getPageSize()

    // Avoid creating tiny tail fragments: only split when remainder is not too small.
    private static final int REMAINDER_SPLIT_RATIO_DIVISOR = 8; // split only if remaining >= allocSize/8

    // Limit free list scan to avoid O(n) overhead in hot path
    // Increased from 16 to 64 for better memory reuse
    private static final int MAX_FREE_LIST_SCAN = 64;

    // Quarantine window: delay releasing empty slabs to avoid same-call-chain dangling access and reduce churn
    private final Deque<Integer> pendingRelease = new ArrayDeque<>();
    private boolean[] slabQuarantined = new boolean[16];

    // ---- New tuning knobs for quarantine and scanning ----
    private static final int DRAIN_BUDGET_PER_CALL = 4;              // drain up to N pages per safe point
    private static final int MAX_QUARANTINE_PAGES = 256;             // threshold to trigger extra draining (increased for sliding window)
    private static final int ADAPTIVE_SCAN_UPPER_BOUND = 128;        // larger scan when about to grow memory
    
    // Operation counter for throttled draining
    private int opCounter = 0;
    private static final int DRAIN_CHECK_INTERVAL = 64;              // only check drain every N operations

    /**
     * Size classes for free list allocation strategy.
     */
    private enum SizeClass {
        TINY(0, 32),           // <= 32 bytes
        SMALL(32, 128),        // 33-128 bytes
        MEDIUM(128, 512),      // 129-512 bytes
        LARGE(512, 2048),      // 513-2048 bytes
        XLARGE(2048, Integer.MAX_VALUE);  // > 2048 bytes

        @SuppressWarnings("unused")
        final int minSize;  // Reserved for future size range validation
        final int maxSize;

        SizeClass(int minSize, int maxSize) {
            this.minSize = minSize;
            this.maxSize = maxSize;
        }

        static SizeClass getSizeClass(int size) {
            if (size <= TINY.maxSize) return TINY;
            if (size <= SMALL.maxSize) return SMALL;
            if (size <= MEDIUM.maxSize) return MEDIUM;
            if (size <= LARGE.maxSize) return LARGE;
            return XLARGE;
        }
    }

    /**
     * Free block representation for FREE_LIST strategy.
     */
    private static class FreeBlock {
        final long address;
        final int size;
        FreeBlock next;

        FreeBlock(long address, int size) {
            this.address = address;
            this.size = size;
            this.next = null;
        }
    }

    private final MemoryManagerAllocator allocator;
    private final List<MemorySegment> segments;
    private final List<List<MemorySegment>> originalAllocations;

    // Current allocation segment
    private MemorySegment currentSegment;
    private int currentOffset;
    private int currentSlabIndex = -1; // 当前写入的slab索引

    // Pre-allocation tracking: next pre-allocated segment to use when current is full
    private int nextPreAllocIndex = -1;  // Index of next pre-allocated segment to use (-1 if none available)

    // Per-slab usage and index reuse structures
    // private final List<Integer> slabUsedBytes = new ArrayList<>();
    private int[] slabUsedBytes = new int[16];
    private final Deque<Integer> freeSlabIndices = new ArrayDeque<>();

    // Free list data structures
    private final FreeBlock[] freeListHeads; // 使用数组按ordinal索引
    private long totalFreedMemory;
    private int totalFreeBlocks;

    // Statistics
    private long totalAllocated;
    private int activeEntries;
    private boolean closed;

    /**
     * Creates an Entry Arena (FREE_LIST allocation strategy).
     */
    public EntryArena(MemoryManagerAllocator allocator) {
        this(allocator, 0);
    }

    /**
     * Creates an Entry Arena with optional memory pre-allocation.
     *
     * @param allocator The memory allocator to use
     * @param initialSizeBytes Initial memory to pre-allocate (0 for no pre-allocation)
     */
    public EntryArena(MemoryManagerAllocator allocator, long initialSizeBytes) {
        this.allocator = allocator;
        this.segments = new ArrayList<>();
        this.originalAllocations = new ArrayList<>();
        this.totalAllocated = 0;
        this.activeEntries = 0;
        this.closed = false;
        this.segmentSize = allocator.getPageSize(); // Align slab size to page size to avoid over-allocation

        // Initialize free list structures
        this.freeListHeads = new FreeBlock[SizeClass.values().length];
        Arrays.fill(freeListHeads, null);
        this.totalFreedMemory = 0;
        this.totalFreeBlocks = 0;

        // Allocate initial segment
        allocateNewSegment();

        // Pre-allocate additional segments if requested
        if (initialSizeBytes > 0) {
            preAllocateSegments(initialSizeBytes);
        }
    }

    /**
     * Pre-allocates memory segments to reduce runtime malloc overhead.
     * Pre-allocated segments will be used when current segment is full.
     *
     * @param totalBytes Total bytes to pre-allocate
     */
    private void preAllocateSegments(long totalBytes) {
        int segmentsToAllocate = (int) Math.ceil((double) totalBytes / segmentSize);
        int preAllocated = 0;
        
        // Record current segment as the starting point (already allocated by constructor)
        int startSegmentIndex = currentSlabIndex;

        LOG.info("EntryArena: Pre-allocating {} segments ({} bytes each, total {} bytes)",
                segmentsToAllocate, segmentSize, totalBytes);

        for (int i = 0; i < segmentsToAllocate; i++) {
            if (allocateNewSegment()) {
                preAllocated++;
                // Note: allocateNewSegment already adds segment to segments list
                // and sets currentSlabIndex to the new segment.
                // We just continue to allocate more.
            } else {
                LOG.warn("EntryArena: Pre-allocation stopped after {} segments (failed to allocate more)",
                        preAllocated);
                break;
            }
        }

        // Set nextPreAllocIndex to point to the first pre-allocated segment
        // (which is the segment after the initial one)
        if (preAllocated > 0) {
            nextPreAllocIndex = startSegmentIndex + 1;
        }

        // Reset to use the first segment for allocation
        // All subsequent segments are already in segments list and ready for use
        if (!segments.isEmpty()) {
            currentSlabIndex = startSegmentIndex;
            currentSegment = segments.get(startSegmentIndex);
            currentOffset = 0;
        }

        LOG.info("EntryArena: Pre-allocated {} segments, total {} bytes, nextPreAllocIndex={}",
                preAllocated, (long) preAllocated * segmentSize, nextPreAllocIndex);
    }

    /**
     * Stores a new entry and returns its address.
     *
     * @param hash Complete hash value to store in entry
     * @param keyBytes Serialized key bytes
     * @param namespaceBytes Serialized namespace bytes
     * @param valueBytes Serialized value bytes
     * @return Entry address (slab_index << 32 | offset), or 0 if allocation failed
     */
    public long putEntry(int hash, byte[] keyBytes, byte[] namespaceBytes, byte[] valueBytes) {
        if (keyBytes == null || namespaceBytes == null || valueBytes == null) {
            return 0;
        }
        // Delegate to the more efficient buffer+length version
        return putEntry(hash, keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length, valueBytes, valueBytes.length);
    }

    /**
     * This is for EntryArena Tests only - computes hash internally.
     * Not recommended for production use due to double hashing overhead.
     */
    public long putEntry(byte[] keyBytes, byte[] namespaceBytes, byte[] valueBytes) {
        if (keyBytes == null || namespaceBytes == null || valueBytes == null) {
            return 0;
        }
        int hash = HashFunctions.compositeHash(keyBytes, namespaceBytes);
        // Delegate to the more efficient buffer+length version
        return putEntry(hash, keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length, valueBytes, valueBytes.length);
    }

    /**
     * Stores a new entry and returns its address (buffer+length 重载，避免复制 DataOutputSerializer 缓冲区）。
     */
    public long putEntry(int hash, byte[] keyBuffer, int keyLen, byte[] namespaceBuffer, int namespaceLen, byte[] valueBuffer, int valueLen) {
        if (closed || keyBuffer == null || namespaceBuffer == null || valueBuffer == null) {
            return 0;
        }
        // Validate input sizes and lengths
        if (keyLen < 0 || keyLen > keyBuffer.length || namespaceLen < 0 || namespaceLen > namespaceBuffer.length ||
                valueLen < 0 || valueLen > valueBuffer.length) {
            return 0;
        }
        if (keyLen > 16 * 1024 || namespaceLen > 16 * 1024 || valueLen > 256 * 1024) {
            return 0;
        }

        int entrySize = calculateEntrySize(keyLen, namespaceLen, valueLen);
        long address = allocateEntry(entrySize);
        if (address == 0) {
            return 0;
        }

        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        // header - store as 4 separate ints to ensure correct byte order for better portability
        segment.putInt(offset + HASH_OFFSET, hash);
        segment.putInt(offset + KEY_LEN_OFFSET, keyLen);
        segment.putInt(offset + NAMESPACE_LEN_OFFSET, namespaceLen);
        segment.putInt(offset + VALUE_LEN_OFFSET, valueLen);
        // body
        int dataOffset = offset + ENTRY_HEADER_SIZE;
        segment.put(dataOffset, keyBuffer, 0, keyLen);
        dataOffset += keyLen;
        segment.put(dataOffset, namespaceBuffer, 0, namespaceLen);
        dataOffset += namespaceLen;
        segment.put(dataOffset, valueBuffer, 0, valueLen);
        activeEntries++;
        // periodic/quarantine threshold based draining
        maybeDrainAfterOp();
        return address;
    }

    /**
     * Updates an existing entry's value.
     * First attempts in-place update if the new value fits in existing space,
     * otherwise allocates new entry and frees the old one into free list.
     */
    public long updateEntry(long address, byte[] valueBytes) {
        if (valueBytes == null) {
            return 0;
        }
        // Delegate to the more efficient buffer+length version
        return updateEntry(address, valueBytes, valueBytes.length);
    }

    /**
     * Updates an existing entry's value using buffer+length (避免复制 DataOutputSerializer 缓冲区).
     * First attempts in-place update if the new value fits in existing space,
     * otherwise allocates new entry and frees the old one into free list.
     */
    public long updateEntry(long address, byte[] valueBuffer, int valueLen) {
        if (address == 0 || valueBuffer == null) {
            return 0;
        }

        if (valueLen > 256 * 1024) {
            return 0;
        }

        // First try in-place update
        if (updateValueInPlace(address, valueBuffer, valueLen)) {
            // In-place update succeeded, return the same address
            maybeDrainAfterOp();
            return address;
        }

        // In-place update failed, fall back to traditional approach
        int hash = getHash(address);
        byte[] keyBytes = getKeyBytes(address);
        byte[] namespaceBytes = getNamespaceBytes(address);

        // Allocate new entry using buffer version with preserved hash
        long newAddress = putEntry(hash, keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length, valueBuffer, valueLen);

        if (newAddress != 0) {
            // Free the old entry into free list
            removeEntry(address);
        }

        return newAddress;
    }

    /**
     * Attempts to update the value in place using buffer+length if the new value fits in the existing space.
     * Returns true if successful, false if a new allocation is needed.
     */
    public boolean updateValueInPlace(long address, byte[] newValueBuffer, int newValueLen) {
        if (closed || address == 0 || newValueBuffer == null) {
            return false;
        }

        // Validate input
        if (newValueLen < 0 || newValueLen > newValueBuffer.length) {
            return false;
        }

        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        int currentValueLen = segment.getInt(offset + VALUE_LEN_OFFSET);

        // Check if new value fits in available space
        if (newValueLen <= currentValueLen) {
            // Update value length
            segment.putInt(offset + VALUE_LEN_OFFSET, newValueLen);

            // Update value data
            int valueOffset = offset + ENTRY_HEADER_SIZE + keyLen + namespaceLen;
            segment.put(valueOffset, newValueBuffer, 0, newValueLen);

            return true;
        }

        return false;
    }

    /**
     * Reads an entry's key bytes.
     */
    public byte[] getKeyBytes(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);

        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        if (keyLen <= 0 || keyLen > 16 * 1024) {
            return keyLen == 0 ? new byte[0] : null;
        }

        byte[] keyBytes = new byte[keyLen];
        segment.get(offset + ENTRY_HEADER_SIZE, keyBytes);
        return keyBytes;
    }

    /**
     * Reads an entry's namespace bytes.
     */
    public byte[] getNamespaceBytes(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);

        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);

        if (namespaceLen <= 0 || namespaceLen > 16 * 1024) {
            return namespaceLen == 0 ? new byte[0] : null;
        }

        byte[] namespaceBytes = new byte[namespaceLen];
        segment.get(offset + ENTRY_HEADER_SIZE + keyLen, namespaceBytes);
        return namespaceBytes;
    }

    /**
     * Reads an entry's value bytes.
     */
    public byte[] getValueBytes(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);

        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int namespaceLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        int valueLen = segment.getInt(offset + VALUE_LEN_OFFSET);

        if (valueLen <= 0 || valueLen > 256 * 1024) {
            return valueLen == 0 ? new byte[0] : null;
        }

        byte[] valueBytes = new byte[valueLen];
        segment.get(offset + ENTRY_HEADER_SIZE + keyLen + namespaceLen, valueBytes);
        return valueBytes;
    }

    /**
     * Reads an entry's stored hash value.
     */
    public int getHash(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        return segment.getInt(offset + HASH_OFFSET);
    }

    /**
     * Checks if key and namespace match for given entry.
     * It is recommended to use the zero-copy version for better performance.
     */
    public boolean matchesKey(long address, byte[] keyBytes, byte[] namespaceBytes) {
        if (address == 0 || keyBytes == null || namespaceBytes == null) {
            return false;
        }
        // Delegate to the zero-copy version for better performance
        return matchesKey(address, keyBytes, keyBytes.length, namespaceBytes, namespaceBytes.length);
    }

    /**
     * Zero-copy matches check with external buffers and explicit lengths.
     */
    public boolean matchesKey(long address, byte[] keyBuffer, int keyLen, byte[] namespaceBuffer, int namespaceLen) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);

        int storedKeyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int storedNsLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        if (storedKeyLen != keyLen || storedNsLen != namespaceLen) {
            return false;
        }
        int dataOffset = offset + ENTRY_HEADER_SIZE;
        // compare key
        if (!equalsSegmentBytes(segment, dataOffset, keyBuffer, keyLen)) {
            return false;
        }
        dataOffset += storedKeyLen;
        // compare namespace
        return equalsSegmentBytes(segment, dataOffset, namespaceBuffer, namespaceLen);
    }

    private static boolean equalsSegmentBytes(MemorySegment seg, int segOffset, byte[] arr, int len) {
        int i = 0;
        // we assume unaligned accesses are supported.
        // Compare 8 bytes at a time.
        while (i + 8 <= len) {
            long v1 = seg.getLong(segOffset + i);
            long v2 = (arr[i] & 0xFF)
                    | ((long) (arr[i + 1] & 0xFF) << 8)
                    | ((long) (arr[i + 2] & 0xFF) << 16)
                    | ((long) (arr[i + 3] & 0xFF) << 24)
                    | ((long) (arr[i + 4] & 0xFF) << 32)
                    | ((long) (arr[i + 5] & 0xFF) << 40)
                    | ((long) (arr[i + 6] & 0xFF) << 48)
                    | ((long) (arr[i + 7] & 0xFF) << 56);
            if (v1 != v2) {
                return false;
            }
            i += 8;
        }
        // cover the last (len % 8) elements.
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
        if (i + 2 <= len) {
            short v1 = seg.getShort(segOffset + i);
            short v2 = (short) ((arr[i] & 0xFF) | ((arr[i + 1] & 0xFF) << 8));
            if (v1 != v2) {
                return false;
            }
            i += 2;
        }
        if (i < len) return seg.get(segOffset + i) == arr[i];
        return true;
    }

    // ==== allocation helpers ====
    private static int calculateEntrySize(int keyLen, int namespaceLen, int valueLen) {
        int dataSize = ENTRY_HEADER_SIZE + keyLen + namespaceLen + valueLen;
        return (Math.max(dataSize, MIN_ENTRY_SIZE) + ALIGNMENT - 1) & (-ALIGNMENT);
    }

    private void ensureSlabCapacity(int index) {
        if (index < slabUsedBytes.length && index < slabQuarantined.length) return;
        int newLen = Math.max(slabUsedBytes.length, slabQuarantined.length);
        while (newLen <= index) { newLen <<= 1; }
        slabUsedBytes = Arrays.copyOf(slabUsedBytes, newLen);
        slabQuarantined = Arrays.copyOf(slabQuarantined, newLen);
    }

    private boolean allocateNewSegment() {
        if (closed) {
            return false;
        }
        try {
            // release quarantined pages at a safe point
            tryDrainQuarantine();

            // request exactly one page-aligned slab
            List<MemorySegment> newSegments = allocator.allocate(segmentSize);
            if (newSegments.isEmpty()) {
                return false;
            }
            // track the allocation handle for precise release later
            int index;
            if (!freeSlabIndices.isEmpty()) {
                index = freeSlabIndices.pollFirst();
                // ensure capacity for aligned lists
                while (originalAllocations.size() <= index) originalAllocations.add(null);
                while (segments.size() <= index) segments.add(null);
                ensureSlabCapacity(index);
                // Store the original List object for proper release tracking with IdentityHashMap
                originalAllocations.set(index, newSegments);
                segments.set(index, newSegments.get(0));
                slabUsedBytes[index] = 0;
                slabQuarantined[index] = false;
            } else {
                index = segments.size();
                segments.add(newSegments.get(0));
                // Store the original List object for proper release tracking with IdentityHashMap
                originalAllocations.add(newSegments);
                ensureSlabCapacity(index);
                slabUsedBytes[index] = 0;
                slabQuarantined[index] = false;
            }

            currentSegment = segments.get(index);
            currentOffset = 0;
            currentSlabIndex = index;
            return true;
        } catch (MemoryAllocationException e) {
            return false;
        }
    }

    private long allocateEntry(int size) {
        // Skip drain here - will drain only when allocation fails or in allocateNewSegment
        
        long reused = allocateFromFreeListBounded(size, EntryArena.MAX_FREE_LIST_SCAN);
        if (reused != 0) { return reused; }

        long addr = allocateFromCurrentSegment(size);
        if (addr != 0) { return addr; }

        return linearAllocate(size);
    }

    private long linearAllocate(int size) {
        long addr = allocateFromCurrentSegment(size);
        if (addr != 0) { return addr; }

        if (size > segmentSize) { return 0; }

        // Before growing memory, do a second-chance broader free-list scan
        long retry = allocateFromFreeListBounded(size, ADAPTIVE_SCAN_UPPER_BOUND);
        if (retry != 0) { return retry; }

        // Try to use next pre-allocated segment if available
        if (useNextPreAllocatedSegment()) {
            addr = allocateFromCurrentSegment(size);
            if (addr != 0) { return addr; }
        }

        // Try to allocate a new page; if it fails, aggressively drain quarantined pages and retry once
        if (!allocateNewSegment()) {
            drainAllQuarantined();
            if (!allocateNewSegment()) { return 0; }
        }

        return allocateFromCurrentSegment(size);
    }

    /**
     * Tries to switch to the next pre-allocated segment.
     * @return true if switched to a pre-allocated segment, false if none available
     */
    private boolean useNextPreAllocatedSegment() {
        if (nextPreAllocIndex >= 0 && nextPreAllocIndex < segments.size()) {
            MemorySegment nextSeg = segments.get(nextPreAllocIndex);
            if (nextSeg != null) {
                currentSlabIndex = nextPreAllocIndex;
                currentSegment = nextSeg;
                currentOffset = 0;
                ensureSlabCapacity(currentSlabIndex);
                slabUsedBytes[currentSlabIndex] = 0;
                nextPreAllocIndex++;
                // Check if we've used all pre-allocated segments
                if (nextPreAllocIndex >= segments.size()) {
                    nextPreAllocIndex = -1;  // No more pre-allocated segments
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to allocate from the current segment if there's enough space.
     * Returns the allocated address or 0 if allocation failed.
     */
    private long allocateFromCurrentSegment(int size) {
        if (currentSegment != null && currentOffset + size <= currentSegment.size()) {
            int slabIndex = currentSlabIndex;
            long addr = ((long)(slabIndex + 1) << 32) | (currentOffset + 1);
            currentOffset += size;
            totalAllocated += size;
            // track per-slab usage
            ensureSlabCapacity(slabIndex);
            slabUsedBytes[slabIndex] += size;
            return addr;
        }
        return 0;
    }

    private long allocateFromFreeListBounded(int size, int maxScan) {
        SizeClass sc = SizeClass.getSizeClass(size);
        long addr = scanFreeList(sc, size, maxScan);
        if (addr != 0) { return addr; }
        int remainProbe = Math.max(1, maxScan / 4);
        for (SizeClass larger : SizeClass.values()) {
            if (larger.ordinal() > sc.ordinal()) {
                addr = scanFreeList(larger, size, remainProbe);
                if (addr != 0) { return addr; }
            }
        }
        return 0;
    }

    private long scanFreeList(SizeClass sc, int size, int maxScan) {
        FreeBlock prev = null, cur = freeListHeads[sc.ordinal()];
        int scanned = 0;
        while (cur != null && scanned < maxScan) {
            if (cur.size >= size) {
                if (prev == null) { freeListHeads[sc.ordinal()] = cur.next; } else { prev.next = cur.next; }
                totalFreeBlocks--; totalFreedMemory -= cur.size;
                int remaining = cur.size - size;
                // split only when remaining is large enough to hold a minimum entry and not a tiny tail
                if (remaining >= MIN_ENTRY_SIZE && remaining >= Math.max(MIN_ENTRY_SIZE, size / REMAINDER_SPLIT_RATIO_DIVISOR)) {
                    addToFreeList(cur.address + size, remaining);
                }
                // update per-slab usage for the reused block
                int slabIndex = getSlabIndex(cur.address);
                ensureSlabCapacity(slabIndex);
                slabUsedBytes[slabIndex] += size;
                return cur.address;
            }
            prev = cur; cur = cur.next; scanned++;
        }
        return 0;
    }

    private void addToFreeList(long address, int size) {
        // only add blocks that can at least serve a minimal entry allocation
        if (size < MIN_ENTRY_SIZE) { return; }
        SizeClass sc = SizeClass.getSizeClass(size);
        FreeBlock nb = new FreeBlock(address, size);
        nb.next = freeListHeads[sc.ordinal()];
        freeListHeads[sc.ordinal()] = nb;
        totalFreeBlocks++;
        totalFreedMemory += size;
    }

    public int getEntrySize(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);

        int k = segment.getInt(offset + KEY_LEN_OFFSET);
        int n = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        int v = segment.getInt(offset + VALUE_LEN_OFFSET);
        return calculateEntrySize(k, n, v);
    }

    public void removeEntry(long address) {
        int sz = getEntrySize(address);
        if (sz > 0) {
            addToFreeList(address, sz);
            // decrease per-slab usage and free page if empty
            int slabIndex = getSlabIndex(address);
            ensureSlabCapacity(slabIndex);
            int used = slabUsedBytes[slabIndex] - sz;
            slabUsedBytes[slabIndex] = used;
            if (used == 0) {
                freeEmptySlabIfPossible(slabIndex);
            }
        }
        activeEntries--;
        // periodic/quarantine threshold based draining
        maybeDrainAfterOp();
    }

    private void freeEmptySlabIfPossible(int slabIndex) {
        // do not free current slab to avoid churn; purge its free-list blocks and reset its offset
        if (slabIndex == currentSlabIndex) {
            // ensure no stale blocks from this slab remain in free lists
            purgeFreeListForSlab(slabIndex);
            currentOffset = 0;
            return;
        }
        // purge free-list blocks from this slab to avoid handing out invalid addresses
        purgeFreeListForSlab(slabIndex);
        // delay actual release: keep segment readable for a short window
        ensureSlabCapacity(slabIndex);
        if (!slabQuarantined[slabIndex]) {
            slabQuarantined[slabIndex] = true;
            pendingRelease.addLast(slabIndex);
        }
        // Skip immediate drain here - let maybeDrainAfterOp handle it with throttling
        // slabUsedBytes[slabIndex] already 0; segment remains until drained
    }

    // Replace previous full-drain with incremental draining at safe points
    private void tryDrainQuarantine() {
        drainQuarantineBudgeted(DRAIN_BUDGET_PER_CALL);
    }

    private void drainQuarantineBudgeted(int budget) {
        int drained = 0;
        while (!pendingRelease.isEmpty() && drained < budget) {
            drainOneQuarantined();
            drained++;
        }
    }

    private void drainAllQuarantined() {
        while (!pendingRelease.isEmpty()) {
            drainOneQuarantined();
        }
    }

    private void maybeDrainAfterOp() {
        // Throttled draining: only check every N operations to reduce overhead
        opCounter++;
        if ((opCounter & (DRAIN_CHECK_INTERVAL - 1)) != 0) {
            return;  // Skip check unless we hit the interval
        }
        
        // Only drain when quarantine queue is very large
        if (pendingRelease.size() > MAX_QUARANTINE_PAGES) {
            int excess = pendingRelease.size() - MAX_QUARANTINE_PAGES;
            drainQuarantineBudgeted(Math.min(excess, DRAIN_BUDGET_PER_CALL));
        }
    }

    private void drainOneQuarantined() {
        Integer idx = pendingRelease.pollFirst();
        if (idx == null) return;
        List<MemorySegment> handle = (idx < originalAllocations.size()) ? originalAllocations.get(idx) : null;
        if (handle != null) {
            allocator.release(handle);
        }
        if (idx < segments.size()) {
            segments.set(idx, null);
        }
        if (idx < originalAllocations.size()) {
            originalAllocations.set(idx, null);
        }
        ensureSlabCapacity(idx);
        slabQuarantined[idx] = false;
        slabUsedBytes[idx] = 0;
        freeSlabIndices.addLast(idx);
    }

    @SuppressWarnings("null")  // tail is guaranteed non-null when newHead is non-null
    private void purgeFreeListForSlab(int slabIndex) {
        for (int i = 0; i < freeListHeads.length; i++) {
            FreeBlock newHead = null;
            FreeBlock tail = null;
            FreeBlock cur = freeListHeads[i];
            while (cur != null) {
                FreeBlock next = cur.next;
                if (getSlabIndex(cur.address) == slabIndex) {
                    // drop this block, update stats
                    totalFreeBlocks--; totalFreedMemory -= cur.size;
                } else {
                    if (newHead == null) { newHead = cur; tail = cur; tail.next = null; }
                    else { 
                        tail.next = cur; tail = cur; tail.next = null; 
                    }
                }
                cur = next;
            }
            freeListHeads[i] = newHead;
        }
    }

    /**
     * Returns a Slice of the value for zero-copy deserialization.
     */
    public MemorySegmentSlice getValueSlice(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int nsLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        int valLen = segment.getInt(offset + VALUE_LEN_OFFSET);
        if (valLen < 0 || valLen > 256 * 1024) {
            return null;
        }
        int off = offset + ENTRY_HEADER_SIZE + keyLen + nsLen;
        return new MemorySegmentSlice(segment, off, valLen);
    }

    /**
     * Returns a Slice of the key (optional helper for iterations).
     */
    public MemorySegmentSlice getKeySlice(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        if (keyLen < 0 || keyLen > 16 * 1024) {
            return null;
        }
        int off = offset + ENTRY_HEADER_SIZE;
        return new MemorySegmentSlice(segment, off, keyLen);
    }

    /**
     * Returns a Slice of the namespace (optional helper for iterations).
     */
    public MemorySegmentSlice getNamespaceSlice(long address) {
        int slabIndex = getSlabIndex(address);
        int offset = getOffset(address);
        MemorySegment segment = segments.get(slabIndex);
        int keyLen = segment.getInt(offset + KEY_LEN_OFFSET);
        int nsLen = segment.getInt(offset + NAMESPACE_LEN_OFFSET);
        if (nsLen < 0 || nsLen > 16 * 1024) {
            return null;
        }
        int off = offset + ENTRY_HEADER_SIZE + keyLen;
        return new MemorySegmentSlice(segment, off, nsLen);
    }

    private static int getSlabIndex(long address) {
        return (int) (address >>> 32) - 1;
    }

    private static int getOffset(long address) {
        return (int) (address & 0xFFFFFFFFL) - 1;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        // Clear free lists
        Arrays.fill(freeListHeads, null);
        totalFreedMemory = 0;
        totalFreeBlocks = 0;

        // drain all quarantined pages first
        while (!pendingRelease.isEmpty()) {
            drainOneQuarantined();
        }

        // Free all original allocations - this ensures proper memory accounting
        for (List<MemorySegment> allocation : originalAllocations) {
            try {
                allocator.release(allocation);
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }

        originalAllocations.clear();
        segments.clear();
        currentSegment = null;
        currentOffset = 0;

        // Reset statistics
        totalAllocated = 0;
        activeEntries = 0;
    }

    /**
     * Gets memory usage statistics.
     */
    public ArenaStats getStats() {
        long slabs = 0;
        for (MemorySegment seg : segments) { if (seg != null) slabs++; }
        long systemMemory = slabs * segmentSize;
        return new ArenaStats(
            systemMemory,
            totalAllocated,
            totalFreedMemory,
            activeEntries,
            totalFreeBlocks,
            calculateMemoryEfficiency()
        );
    }

    /**
     * Calculates memory efficiency as a percentage.
     */
    private double calculateMemoryEfficiency() {
        long slabs = 0;
        for (MemorySegment seg : segments) { if (seg != null) slabs++; }
        long systemMemory = slabs * segmentSize;
        if (systemMemory == 0) {
            return 0.0;
        }
        return (double) totalAllocated / systemMemory * 100.0;
    }

    /**
     * Statistics class for arena memory usage.
     */
    public static class ArenaStats {
        public final long totalSystemMemory;
        public final long totalAllocated;
        public final long totalFreed;
        public final int activeAllocations;
        public final int freeBlocks;
        public final double memoryEfficiency;
        public final double fragmentation;

        public ArenaStats(long totalSystemMemory, long totalAllocated,
                         long totalFreed, int activeAllocations, int freeBlocks, double memoryEfficiency) {
            this.totalSystemMemory = totalSystemMemory;
            this.totalAllocated = totalAllocated;
            this.totalFreed = totalFreed;
            this.activeAllocations = activeAllocations;
            this.freeBlocks = freeBlocks;
            this.memoryEfficiency = memoryEfficiency;
            this.fragmentation = freeBlocks > 0 ? (double) freeBlocks / (activeAllocations + freeBlocks) * 100.0 : 0.0;
        }

        @Override
        public String toString() {
            return String.format(
                "ArenaStats{systemMemory=%d, allocated=%d, freed=%d, " +
                "activeEntries=%d, freeBlocks=%d, efficiency=%.2f%%, fragmentation=%.2f%%}",
                totalSystemMemory, totalAllocated, totalFreed,
                activeAllocations, freeBlocks, memoryEfficiency, fragmentation
            );
        }
    }
}
