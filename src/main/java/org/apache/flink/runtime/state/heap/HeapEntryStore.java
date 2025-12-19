/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Heap-based entry storage using chunked arrays for efficient memory management.
 * 
 * <p>This class stores {@link HeapStateEntry} objects in a chunked array structure,
 * avoiding the need for serialization on the hot path. It provides stable "addresses"
 * (array indices) that can be stored in the off-heap index (L0Table/MainTable).
 * 
 * <p>Architecture:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                     HeapEntryStore                              │
 * ├─────────────────────────────────────────────────────────────────┤
 * │  chunks[0] → [Entry|Entry|Entry|...|Entry]  (65536 slots)       │
 * │  chunks[1] → [Entry|Entry|Entry|...|Entry]  (65536 slots)       │
 * │  chunks[N] → [Entry|Entry|Entry|...|Entry]  (65536 slots)       │
 * ├─────────────────────────────────────────────────────────────────┤
 * │  freeList: [index3, index7, index42, ...]   (stack structure)   │
 * └─────────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * <p>Key Design Points:
 * <ul>
 *   <li><b>Chunked Array</b>: Each chunk holds 2^16 = 65536 entries, avoiding
 *       large contiguous allocations and reducing GC pause times</li>
 *   <li><b>Address = Index + 1</b>: Address 0 is reserved as NULL, so returned
 *       addresses are always > 0</li>
 *   <li><b>Free List (LIFO)</b>: Deleted slots are reused via a stack, providing
 *       O(1) allocation and cache-friendly access patterns</li>
 *   <li><b>Zero Serialization</b>: Objects are stored directly as references</li>
 * </ul>
 * 
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class HeapEntryStore<K, N, S> implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(HeapEntryStore.class);
    
    // ========== Constants ==========
    
    /** 
     * Number of bits for chunk size. Each chunk has 2^16 = 65536 slots.
     * This provides a good balance between chunk granularity and index efficiency.
     */
    private static final int CHUNK_SIZE_BITS = 16;
    
    /** Number of entries per chunk (65536). */
    private static final int CHUNK_SIZE = 1 << CHUNK_SIZE_BITS;
    
    /** Mask for extracting slot index within a chunk. */
    private static final int CHUNK_MASK = CHUNK_SIZE - 1;
    
    /** Initial number of chunks. */
    private static final int INITIAL_CHUNKS = 1;
    
    /** Initial size of the free list array. */
    private static final int INITIAL_FREE_LIST_SIZE = 1024;
    
    // ========== Storage Structure ==========
    
    /** 
     * Chunked array storage: chunks[chunkIndex][slotIndex].
     * Each chunk is lazily allocated when needed.
     */
    private HeapStateEntry<K, N, S>[][] chunks;
    
    /** Current number of allocated chunks. */
    private int chunkCount;
    
    /** Next allocation index (for sequential allocation when free list is empty). */
    private int nextAllocIndex;
    
    // ========== Free Slot Management ==========
    
    /** 
     * Free list stack. Stores indices of deleted slots for reuse.
     * Uses LIFO order for cache-friendly access (recently freed = likely still in cache).
     */
    private int[] freeList;
    
    /** Number of entries in the free list. */
    private int freeCount;
    
    // ========== Statistics ==========
    
    /** Number of active (non-deleted) entries. */
    private int activeEntries;
    
    /** Total number of allocations performed. */
    private long totalAllocations;
    
    /** Total number of removals performed. */
    private long totalRemovals;
    
    // ========== State ==========
    
    /** Whether this store has been closed. */
    private boolean closed;
    
    // ========== Constructors ==========
    
    /**
     * Creates a new HeapEntryStore with default configuration.
     */
    @SuppressWarnings("unchecked")
    public HeapEntryStore() {
        this.chunks = (HeapStateEntry<K, N, S>[][]) new HeapStateEntry[INITIAL_CHUNKS][];
        this.chunks[0] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[CHUNK_SIZE];
        this.chunkCount = 1;
        this.nextAllocIndex = 0;
        
        this.freeList = new int[INITIAL_FREE_LIST_SIZE];
        this.freeCount = 0;
        
        this.activeEntries = 0;
        this.totalAllocations = 0;
        this.totalRemovals = 0;
        this.closed = false;
        
        LOG.debug("[HeapEntryStore] Created with chunk size {}", CHUNK_SIZE);
    }
    
    // ========== Core Operations ==========
    
    /**
     * Allocates a new entry and returns its address.
     * 
     * <p>The address is guaranteed to be > 0 (0 is reserved as NULL).
     * The actual index is address - 1.
     * 
     * @param key the key (must not be null)
     * @param namespace the namespace (must not be null)
     * @param state the state value (can be null)
     * @return the allocated address (always > 0)
     */
    public long allocate(@Nonnull K key, @Nonnull N namespace, @Nullable S state) {
        checkNotClosed();
        
        int index;
        
        // Prefer reusing freed slots (LIFO for cache efficiency)
        if (freeCount > 0) {
            index = freeList[--freeCount];
        } else {
            // Check if we need to expand chunks
            if (nextAllocIndex >= chunkCount * CHUNK_SIZE) {
                expandChunks();
            }
            index = nextAllocIndex++;
        }
        
        // Create and store the entry
        HeapStateEntry<K, N, S> entry = new HeapStateEntry<>(key, namespace, state);
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        chunks[chunkIndex][slotIndex] = entry;
        
        activeEntries++;
        totalAllocations++;
        
        // Return index + 1 to reserve 0 as NULL
        return (long) index + 1;
    }
    
    /**
     * Gets the entry at the given address.
     * 
     * @param address the address (from allocate())
     * @return the entry, or null if the address is invalid or the slot is empty
     */
    @Nullable
    public HeapStateEntry<K, N, S> get(long address) {
        if (address <= 0) {
            return null;
        }
        
        int index = (int) address - 1;
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        
        if (chunkIndex >= chunkCount) {
            return null;
        }
        
        return chunks[chunkIndex][slotIndex];
    }
    
    /**
     * Gets the hash value of the entry at the given address.
     * 
     * @param address the address
     * @return the hash value, or 0 if the address is invalid
     */
    public int getHash(long address) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null ? entry.getHash() : 0;
    }
    
    /**
     * Gets the tag of the entry at the given address.
     * 
     * <p>The tag is the high 16 bits of the hash, used for fast filtering
     * in the off-heap index.
     * 
     * @param address the address
     * @return the tag, or 0 if the address is invalid
     */
    public short getTag(long address) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null ? entry.getTag() : 0;
    }
    
    /**
     * Checks if the entry at the given address matches the key and namespace.
     * 
     * @param address the address
     * @param key the key to match
     * @param namespace the namespace to match
     * @return true if the entry exists and matches
     */
    public boolean matches(long address, @Nonnull K key, @Nonnull N namespace) {
        HeapStateEntry<K, N, S> entry = get(address);
        return entry != null && entry.matches(key, namespace);
    }
    
    /**
     * Updates the state of the entry at the given address.
     * 
     * <p>This is an in-place update; the address remains stable.
     * 
     * @param address the address
     * @param state the new state value
     * @return true if the update was successful (entry exists)
     */
    public boolean updateState(long address, @Nullable S state) {
        HeapStateEntry<K, N, S> entry = get(address);
        if (entry != null) {
            entry.setState(state);
            return true;
        }
        return false;
    }
    
    /**
     * Removes the entry at the given address and adds the slot to the free list.
     * 
     * @param address the address
     */
    public void remove(long address) {
        if (address <= 0) {
            return;
        }
        
        int index = (int) address - 1;
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        
        if (chunkIndex >= chunkCount) {
            return;
        }
        
        if (chunks[chunkIndex][slotIndex] != null) {
            // Clear the slot
            chunks[chunkIndex][slotIndex] = null;
            activeEntries--;
            totalRemovals++;
            
            // Add to free list (expand if necessary)
            if (freeCount >= freeList.length) {
                freeList = Arrays.copyOf(freeList, freeList.length * 2);
            }
            freeList[freeCount++] = index;
        }
    }
    
    // ========== Iteration Support ==========
    
    /**
     * Returns the maximum address that has been allocated.
     * 
     * <p>Note: Not all addresses up to this value may contain entries
     * (some may have been removed).
     * 
     * @return the maximum allocated address (or 0 if empty)
     */
    public long getMaxAddress() {
        return nextAllocIndex;
    }
    
    /**
     * Gets an entry by its raw index (0-based, without the +1 offset).
     * 
     * <p>This is useful for iteration. Use indices from 0 to nextAllocIndex-1.
     * 
     * @param index the raw index
     * @return the entry, or null if not present
     */
    @Nullable
    HeapStateEntry<K, N, S> getByIndex(int index) {
        if (index < 0 || index >= nextAllocIndex) {
            return null;
        }
        int chunkIndex = index >> CHUNK_SIZE_BITS;
        int slotIndex = index & CHUNK_MASK;
        return chunks[chunkIndex][slotIndex];
    }
    
    // ========== Chunk Management ==========
    
    /**
     * Expands the chunk array when more capacity is needed.
     */
    @SuppressWarnings("unchecked")
    private void expandChunks() {
        int newChunkCount = chunkCount + 1;
        
        // Expand the chunks array if needed
        if (newChunkCount > chunks.length) {
            int newLength = Math.max(chunks.length * 2, newChunkCount);
            chunks = Arrays.copyOf(chunks, newLength);
        }
        
        // Allocate the new chunk
        chunks[chunkCount] = (HeapStateEntry<K, N, S>[]) new HeapStateEntry[CHUNK_SIZE];
        chunkCount = newChunkCount;
        
        LOG.debug("[HeapEntryStore] Expanded to {} chunks (capacity: {})", 
            chunkCount, (long) chunkCount * CHUNK_SIZE);
    }
    
    // ========== Statistics ==========
    
    /**
     * Returns the number of active (non-deleted) entries.
     * 
     * @return the active entry count
     */
    public int getActiveEntries() {
        return activeEntries;
    }
    
    /**
     * Returns the total capacity (number of slots across all chunks).
     * 
     * @return the total capacity
     */
    public long getCapacity() {
        return (long) chunkCount * CHUNK_SIZE;
    }
    
    /**
     * Returns the number of slots in the free list.
     * 
     * @return the free slot count
     */
    public int getFreeSlotCount() {
        return freeCount;
    }
    
    /**
     * Returns the total number of allocations performed.
     * 
     * @return the total allocation count
     */
    public long getTotalAllocations() {
        return totalAllocations;
    }
    
    /**
     * Returns the total number of removals performed.
     * 
     * @return the total removal count
     */
    public long getTotalRemovals() {
        return totalRemovals;
    }
    
    /**
     * Returns the next allocation index (high water mark).
     * 
     * @return the next allocation index
     */
    public int getNextAllocIndex() {
        return nextAllocIndex;
    }
    
    /**
     * Returns the current number of allocated chunks.
     * 
     * @return the chunk count
     */
    public int getChunkCount() {
        return chunkCount;
    }
    
    // ========== Lifecycle ==========
    
    /**
     * Checks if this store has been closed.
     * 
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Throws an exception if this store has been closed.
     */
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("HeapEntryStore has been closed");
        }
    }
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        // Help GC by clearing all references
        for (int i = 0; i < chunkCount; i++) {
            if (chunks[i] != null) {
                Arrays.fill(chunks[i], null);
                chunks[i] = null;
            }
        }
        chunks = null;
        freeList = null;
        
        closed = true;
        
        LOG.debug("[HeapEntryStore] Closed. Stats: allocations={}, removals={}, activeAtClose={}", 
            totalAllocations, totalRemovals, activeEntries);
    }
    
    @Override
    public String toString() {
        return "HeapEntryStore{" +
            "activeEntries=" + activeEntries +
            ", capacity=" + getCapacity() +
            ", chunks=" + chunkCount +
            ", freeSlots=" + freeCount +
            ", closed=" + closed +
            '}';
    }
}
