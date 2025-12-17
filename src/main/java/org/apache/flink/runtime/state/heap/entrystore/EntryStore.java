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
        
        // Allocate value space first
        long valueHandle = valuePool.allocate(valueLen);
        if (valueHandle == NULL_HANDLE) {
            return NULL_HANDLE;
        }
        
        // Write value if provided
        if (valueBuffer != null && valueLen > 0) {
            valuePool.write(valueHandle, valueBuffer, valueLen);
        }
        
        // Allocate key/namespace entry with valueHandle
        long keyAddr = keyNsPool.allocate(hash, keyBuffer, keyLen, nsBuffer, nsLen, valueHandle);
        if (keyAddr == NULL_HANDLE) {
            // Rollback value allocation
            valuePool.free(valueHandle);
            return NULL_HANDLE;
        }
        
        return keyAddr;
    }
    
    /**
     * Updates an entry's value.
     * 
     * <p><b>CRITICAL</b>: This method guarantees that the entry address does NOT change.
     * The caller does not need to update any index pointers after calling this method.
     * 
     * <p>Implementation:
     * <ol>
     *   <li>Try in-place update if new value fits in existing slot</li>
     *   <li>Otherwise, allocate new value slot and update the valueHandle in KeyNsPool</li>
     * </ol>
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
        
        // Get current value handle
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
     * 
     * @param address the entry address
     */
    public void removeEntry(long address) {
        if (address == NULL_HANDLE) {
            return;
        }
        
        // Get and free value first
        long valueHandle = keyNsPool.getValueHandle(address);
        if (valueHandle != NULL_HANDLE) {
            valuePool.free(valueHandle);
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
     */
    public byte[] getValueBytes(long address) {
        if (address == NULL_HANDLE) {
            return null;
        }
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
        long valueHandle = keyNsPool.getValueHandle(address);
        return valuePool.getSlice(valueHandle);
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
