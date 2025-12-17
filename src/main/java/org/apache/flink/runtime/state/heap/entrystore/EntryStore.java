package org.apache.flink.runtime.state.heap.entrystore;

import org.apache.flink.runtime.state.heap.space.MemoryManagerAllocator;
import org.apache.flink.runtime.state.heap.space.MemorySegmentSlice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.state.heap.entrystore.EntryStoreConstants.*;

/**
 * EntryStore: Unified key-value entry storage with key-value separation.
 * 
 * <p>Architecture:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                      EntryStore                                  │
 * ├─────────────────────────────┬───────────────────────────────────┤
 * │        KeyNsPool            │           ValuePool               │
 * │     (键命名空间池)            │           (值池)                  │
 * ├─────────────────────────────┼───────────────────────────────────┤
 * │  Append-only allocation     │  Size-class + bitmap allocation   │
 * │  Segment-level reclamation  │  In-place update when possible    │
 * └─────────────────────────────┴───────────────────────────────────┘
 * </pre>
 * 
 * <p>Key Design Points:
 * <ul>
 *   <li><b>Address Stability</b>: updateValue() guarantees stable addresses - 
 *       the returned boolean indicates success, not a new address</li>
 *   <li><b>Key-Value Separation</b>: Keys/namespaces are stored separately from values,
 *       allowing value updates without affecting index pointers</li>
 *   <li><b>Zero-Copy Support</b>: Slice accessors for efficient memory access</li>
 * </ul>
 * 
 * <p>Entry Address Format:
 * The address returned by {@link #allocateEntry} points to the KeyNsPool entry,
 * which contains a valueHandle pointing to the ValuePool entry.
 */
public class EntryStore implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(EntryStore.class);
    
    // ========== Sub-pools ==========
    
    private final KeyNsPool keyNsPool;
    private final ValuePool valuePool;
    private final MemoryManagerAllocator allocator;
    
    // ========== State ==========
    
    private boolean closed;
    
    // ========== Constructors ==========
    
    /**
     * Creates an EntryStore with default configuration.
     * 
     * @param allocator the memory allocator to use
     */
    public EntryStore(MemoryManagerAllocator allocator) {
        this(allocator, 0);
    }
    
    /**
     * Creates an EntryStore with optional pre-allocation.
     * 
     * @param allocator the memory allocator to use
     * @param initialSizeBytes initial memory to pre-allocate (split between pools)
     */
    public EntryStore(MemoryManagerAllocator allocator, long initialSizeBytes) {
        this.allocator = allocator;
        
        // Split pre-allocation between pools (50% each)
        long keyNsPreAlloc = initialSizeBytes / 2;
        long valuePreAlloc = initialSizeBytes / 2;
        
        this.keyNsPool = new KeyNsPool(allocator, keyNsPreAlloc);
        this.valuePool = new ValuePool(allocator, valuePreAlloc);
        this.closed = false;
        
        LOG.debug("EntryStore created with allocator page size: {} bytes", allocator.getPageSize());
    }
    
    // ========== Write Operations ==========
    
    /**
     * Allocates and stores a new entry.
     * 
     * <p>If valueBuffer is null but valueLen > 0, space is reserved for the value
     * but not written. Use {@link #getValueSlice(long)} to get the value region
     * and write directly for zero-copy optimization.
     * 
     * @param hash the pre-computed hash value
     * @param keyBuffer the key bytes buffer
     * @param keyLen the key length
     * @param nsBuffer the namespace bytes buffer
     * @param nsLen the namespace length
     * @param valueBuffer the value bytes buffer (can be null to reserve space)
     * @param valueLen the value length
     * @return entry address, or 0 on failure
     */
    public long allocateEntry(int hash, 
                              byte[] keyBuffer, int keyLen,
                              byte[] nsBuffer, int nsLen,
                              byte[] valueBuffer, int valueLen) {
        if (closed) {
            return NULL_HANDLE;
        }
        
        // Validate inputs
        if (keyBuffer == null || nsBuffer == null) {
            return NULL_HANDLE;
        }
        if (keyLen < 0 || keyLen > keyBuffer.length || keyLen > MAX_KEY_SIZE) {
            return NULL_HANDLE;
        }
        if (nsLen < 0 || nsLen > nsBuffer.length || nsLen > MAX_NAMESPACE_SIZE) {
            return NULL_HANDLE;
        }
        if (valueLen < 0 || valueLen > MAX_VALUE_SIZE) {
            return NULL_HANDLE;
        }
        if (valueBuffer != null && valueLen > valueBuffer.length) {
            return NULL_HANDLE;
        }
        
        // Check if value can be inlined (≤8 bytes)
        if (valueLen <= INLINE_THRESHOLD) {
            return allocateInlineEntry(hash, keyBuffer, keyLen, nsBuffer, nsLen, valueBuffer, valueLen);
        }
        
        // Pointer mode: allocate value space first
        long valueHandle = valuePool.allocate(valueLen);
        if (valueHandle == NULL_HANDLE) {
            return NULL_HANDLE;
        }
        
        // Write value if provided
        if (valueBuffer != null && valueLen > 0) {
            valuePool.write(valueHandle, valueBuffer, valueLen);
        }
        
        // Allocate key/namespace entry with valueHandle (pointer mode)
        long keyAddr = keyNsPool.allocate(hash, keyBuffer, keyLen, nsBuffer, nsLen, valueHandle);
        if (keyAddr == NULL_HANDLE) {
            // Rollback value allocation
            valuePool.free(valueHandle);
            return NULL_HANDLE;
        }
        
        return keyAddr;
    }
    
    /**
     * Allocates an entry with inline value (≤8 bytes).
     */
    private long allocateInlineEntry(int hash,
                                     byte[] keyBuffer, int keyLen,
                                     byte[] nsBuffer, int nsLen,
                                     byte[] valueBuffer, int valueLen) {
        // Pack value bytes into a long (little-endian)
        long inlineValue = 0L;
        if (valueBuffer != null && valueLen > 0) {
            for (int i = 0; i < valueLen; i++) {
                inlineValue |= ((long) (valueBuffer[i] & 0xFF)) << (i * 8);
            }
        }
        
        return keyNsPool.allocateInline(hash, keyBuffer, keyLen, nsBuffer, nsLen, inlineValue, valueLen);
    }
    
    /**
     * Updates an entry's value.
     * 
     * <p><b>CRITICAL</b>: This method guarantees that the entry address does NOT change.
     * The caller does not need to update any index pointers after calling this method.
     * 
     * <p>Handles 4 mode transitions:
     * <ul>
     *   <li>inline → inline: update in-place if new length ≤8</li>
     *   <li>inline → pointer: allocate in ValuePool, switch mode</li>
     *   <li>pointer → pointer: try in-place, otherwise reallocate</li>
     *   <li>pointer → inline: free ValuePool slot, switch mode</li>
     * </ul>
     * 
     * @param address the entry address (from allocateEntry)
     * @param valueBuffer the new value bytes
     * @param valueLen the new value length
     * @return true if update succeeded, false on error
     */
    public boolean updateValue(long address, byte[] valueBuffer, int valueLen) {
        if (closed || address == NULL_HANDLE || valueBuffer == null) {
            return false;
        }
        
        if (valueLen < 0 || valueLen > valueBuffer.length || valueLen > MAX_VALUE_SIZE) {
            return false;
        }
        
        boolean wasInline = keyNsPool.isInlineMode(address);
        boolean canInline = valueLen <= INLINE_THRESHOLD;
        
        if (wasInline && canInline) {
            // Case 1: inline → inline (in-place update)
            return updateInlineToInline(address, valueBuffer, valueLen);
        } else if (wasInline && !canInline) {
            // Case 2: inline → pointer (allocate in ValuePool)
            return updateInlineToPointer(address, valueBuffer, valueLen);
        } else if (!wasInline && canInline) {
            // Case 3: pointer → inline (free ValuePool, switch mode)
            return updatePointerToInline(address, valueBuffer, valueLen);
        } else {
            // Case 4: pointer → pointer (try in-place or reallocate)
            return updatePointerToPointer(address, valueBuffer, valueLen);
        }
    }
    
    /**
     * Case 1: Update inline value in-place.
     */
    private boolean updateInlineToInline(long address, byte[] valueBuffer, int valueLen) {
        // Pack new value into long
        long newInlineValue = 0L;
        for (int i = 0; i < valueLen; i++) {
            newInlineValue |= ((long) (valueBuffer[i] & 0xFF)) << (i * 8);
        }
        keyNsPool.updateInlineValue(address, newInlineValue, valueLen);
        return true;
    }
    
    /**
     * Case 2: Transition from inline to pointer mode.
     */
    private boolean updateInlineToPointer(long address, byte[] valueBuffer, int valueLen) {
        // Allocate in ValuePool
        long newValueHandle = valuePool.allocate(valueLen);
        if (newValueHandle == NULL_HANDLE) {
            return false;
        }
        
        // Write value
        valuePool.write(newValueHandle, valueBuffer, valueLen);
        
        // Switch to pointer mode
        keyNsPool.switchToPointerMode(address, newValueHandle);
        return true;
    }
    
    /**
     * Case 3: Transition from pointer to inline mode.
     */
    private boolean updatePointerToInline(long address, byte[] valueBuffer, int valueLen) {
        // Get old value handle to free later
        long oldValueHandle = keyNsPool.getValueHandle(address);
        
        // Pack new value into long
        long newInlineValue = 0L;
        for (int i = 0; i < valueLen; i++) {
            newInlineValue |= ((long) (valueBuffer[i] & 0xFF)) << (i * 8);
        }
        
        // Switch to inline mode
        keyNsPool.switchToInlineMode(address, newInlineValue, valueLen);
        
        // Free old value slot (must be after mode switch to ensure consistency)
        if (oldValueHandle != NULL_HANDLE) {
            valuePool.free(oldValueHandle);
        }
        return true;
    }
    
    /**
     * Case 4: Update pointer mode value (try in-place or reallocate).
     */
    private boolean updatePointerToPointer(long address, byte[] valueBuffer, int valueLen) {
        long oldValueHandle = keyNsPool.getValueHandle(address);
        if (oldValueHandle == NULL_HANDLE) {
            return false;
        }
        
        // Try in-place update first
        if (valuePool.updateInPlace(oldValueHandle, valueBuffer, valueLen)) {
            return true;
        }
        
        // In-place update failed, need to reallocate value
        long newValueHandle = valuePool.allocate(valueLen);
        if (newValueHandle == NULL_HANDLE) {
            return false;
        }
        
        // Write new value
        valuePool.write(newValueHandle, valueBuffer, valueLen);
        
        // Update valueHandle in KeyNsPool (address stays the same!)
        keyNsPool.updateValueHandle(address, newValueHandle);
        
        // Free old value slot
        valuePool.free(oldValueHandle);
        
        return true;
    }
    
    /**
     * Removes an entry and frees all associated memory.
     * Handles both inline and pointer modes.
     * 
     * @param address the entry address
     */
    public void removeEntry(long address) {
        if (address == NULL_HANDLE) {
            return;
        }
        
        // Only free value in pointer mode (inline mode has no ValuePool allocation)
        if (!keyNsPool.isInlineMode(address)) {
            long valueHandle = keyNsPool.getValueHandle(address);
            if (valueHandle != NULL_HANDLE) {
                valuePool.free(valueHandle);
            }
        }
        
        // Free key/namespace entry
        keyNsPool.free(address);
    }
    
    // ========== Read Operations ==========
    
    /**
     * Gets the hash value of an entry.
     */
    public int getHash(long address) {
        return keyNsPool.getHash(address);
    }
    
    /**
     * Gets the key bytes of an entry.
     */
    public byte[] getKeyBytes(long address) {
        return keyNsPool.getKeyBytes(address);
    }
    
    /**
     * Gets the namespace bytes of an entry.
     */
    public byte[] getNamespaceBytes(long address) {
        return keyNsPool.getNamespaceBytes(address);
    }
    
    /**
     * Gets the value bytes of an entry.
     * Handles both inline and pointer modes.
     */
    public byte[] getValueBytes(long address) {
        if (address == NULL_HANDLE) {
            return null;
        }
        
        // Check mode
        if (keyNsPool.isInlineMode(address)) {
            return keyNsPool.getInlineValueBytes(address);
        }
        
        // Pointer mode
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.read(valueHandle);
    }
    
    // ========== Zero-Copy Slice Access ==========
    
    /**
     * Gets a zero-copy slice for the key.
     */
    public MemorySegmentSlice getKeySlice(long address) {
        return keyNsPool.getKeySlice(address);
    }
    
    /**
     * Gets a zero-copy slice for the namespace.
     */
    public MemorySegmentSlice getNamespaceSlice(long address) {
        return keyNsPool.getNamespaceSlice(address);
    }
    
    /**
     * Gets a zero-copy slice for the value.
     * Handles both inline and pointer modes.
     * 
     * <p>For inline mode, returns a slice pointing to the valueHandle field
     * in KeyNsPool. For pointer mode, returns a slice from ValuePool.
     * 
     * <p>This can be used for zero-copy value writes:
     * <pre>
     * long addr = entryStore.allocateEntry(hash, key, keyLen, ns, nsLen, null, valueSize);
     * MemorySegmentSlice slice = entryStore.getValueSlice(addr);
     * serializer.serialize(value, slice.segment, slice.offset);
     * </pre>
     */
    public MemorySegmentSlice getValueSlice(long address) {
        if (address == NULL_HANDLE) {
            return null;
        }
        
        // Check mode
        if (keyNsPool.isInlineMode(address)) {
            return keyNsPool.getInlineValueSlice(address);
        }
        
        // Pointer mode
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.getSlice(valueHandle);
    }
    
    /**
     * Checks if entry is in inline mode.
     * 
     * @param address the entry address
     * @return true if inline mode, false if pointer mode
     */
    public boolean isInlineMode(long address) {
        return keyNsPool.isInlineMode(address);
    }
    
    /**
     * Gets the value length of an entry.
     * Handles both inline and pointer modes.
     * 
     * @param address the entry address
     * @return value length, or -1 if error
     */
    public int getValueLen(long address) {
        if (address == NULL_HANDLE) {
            return -1;
        }
        
        if (keyNsPool.isInlineMode(address)) {
            return keyNsPool.getInlineValueLen(address);
        }
        
        // Pointer mode - get from ValuePool
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.getValueLen(valueHandle);
    }
    
    // ========== Key Matching ==========
    
    /**
     * Checks if the key and namespace match the entry at the given address.
     */
    public boolean matchesKey(long address, byte[] keyBuffer, int keyLen, byte[] nsBuffer, int nsLen) {
        return keyNsPool.matchesKey(address, keyBuffer, keyLen, nsBuffer, nsLen);
    }
    
    // ========== Statistics ==========
    
    /**
     * Gets comprehensive statistics about the EntryStore.
     */
    public EntryStoreStats getStats() {
        return EntryStoreStats.builder()
                .keyNsSegmentCount(keyNsPool.getSegmentCount())
                .keyNsActiveSegments(keyNsPool.getActiveSegmentCount())
                .keyNsTotalBytes(keyNsPool.getTotalAllocatedBytes())
                .keyNsActiveEntries(keyNsPool.getActiveEntries())
                .valueRunCount(valuePool.getRunCount())
                .valueActiveRuns(valuePool.getActiveRunCount())
                .valueTotalBytes(valuePool.getTotalAllocatedBytes())
                .valueActiveCount(valuePool.getActiveValues())
                .largeObjectCount(valuePool.getLargeObjectCount())
                .largeObjectBytes(valuePool.getLargeObjectBytes())
                .build();
    }
    
    /**
     * Gets the number of active entries.
     */
    public int getActiveEntries() {
        return keyNsPool.getActiveEntries();
    }
    
    /**
     * Releases all empty segments/runs to reduce memory footprint.
     * 
     * @return total bytes released
     */
    public long releaseEmptyMemory() {
        long released = 0;
        released += keyNsPool.releaseEmptySegments();
        released += valuePool.releaseEmptyRuns();
        return released;
    }
    
    // ========== Lifecycle ==========
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        closed = true;
        
        try {
            keyNsPool.close();
        } catch (Exception e) {
            LOG.warn("Error closing KeyNsPool", e);
        }
        
        try {
            valuePool.close();
        } catch (Exception e) {
            LOG.warn("Error closing ValuePool", e);
        }
        
        LOG.debug("EntryStore closed");
    }
    
    /**
     * Returns true if the store is closed.
     */
    public boolean isClosed() {
        return closed;
    }
}
