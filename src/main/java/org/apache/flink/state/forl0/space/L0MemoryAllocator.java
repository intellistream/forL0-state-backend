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

package org.apache.flink.state.forl0.space;

/**
 * Interface for L0 memory allocation.
 * L0 memory is specialized memory (e.g., CXL memory, PMEM) allocated via JNI native methods.
 *
 * <p>The L0 memory is used exclusively for the L0Table (hot key cache) and has different
 * characteristics from regular off-heap memory:
 * <ul>
 *     <li>May be backed by CXL (Compute Express Link) memory</li>
 *     <li>May be backed by persistent memory (PMEM)</li>
 *     <li>Allocated and managed by native code via JNI</li>
 * </ul>
 *
 * @see NativeL0MemoryAllocator
 */
public interface L0MemoryAllocator extends AutoCloseable {

    /**
     * Allocates L0 memory for the specified number of bytes.
     *
     * @param bytes Number of bytes to allocate
     * @return Allocation handle for the native memory region
     * @throws L0MemoryAllocationException if allocation fails
     */
    L0Allocation allocate(int bytes) throws L0MemoryAllocationException;

    /**
     * Releases previously allocated L0 memory.
     *
     * @param allocation Memory allocation to release
     */
    void release(L0Allocation allocation);

    /**
     * Gets the current L0 memory usage in bytes.
     *
     * @return Used bytes
     */
    long getUsedBytes();

    /**
     * Gets the total L0 memory capacity in bytes.
     * Returns -1 if the capacity is unlimited or unknown.
     *
     * @return Total capacity in bytes, or -1 if unknown
     */
    long getTotalCapacity();

    /**
     * Checks if the allocator is closed.
     *
     * @return true if closed, false otherwise
     */
    boolean isClosed();

    /**
     * Represents an allocated native memory region.
     * Provides direct access to memory addresses without MemorySegment wrapper.
     */
    class L0Allocation {
        /** Array of native memory addresses (one per segment if memory is split). */
        public final long[] addresses;
        /** Size of each segment in bytes. */
        public final int segmentSize;
        /** Total allocated size in bytes. */
        public final int totalSize;
        
        public L0Allocation(long[] addresses, int segmentSize, int totalSize) {
            this.addresses = addresses;
            this.segmentSize = segmentSize;
            this.totalSize = totalSize;
        }
        
        /** Single-segment constructor. */
        public L0Allocation(long address, int size) {
            this.addresses = new long[] { address };
            this.segmentSize = size;
            this.totalSize = size;
        }
    }

    /**
     * Exception thrown when L0 memory allocation fails.
     */
    class L0MemoryAllocationException extends Exception {
        public L0MemoryAllocationException(String message) {
            super(message);
        }

        public L0MemoryAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
