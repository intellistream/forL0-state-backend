package org.apache.flink.runtime.state.heap.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI bridge for native L0 memory allocation.
 * This class provides native methods for allocating and freeing memory
 * using C malloc/free functions.
 *
 * <p>The native library must be loaded before using any native methods.
 * Use {@link #isAvailable()} to check if the native library is loaded.
 *
 * <p>Future versions may replace malloc/free with:
 * <ul>
 *     <li>CXL memory allocation via libcxl</li>
 *     <li>PMEM allocation via libpmem</li>
 *     <li>Custom memory pool allocators</li>
 * </ul>
 */
public final class NativeL0Memory {

    private static final Logger LOG = LoggerFactory.getLogger(NativeL0Memory.class);

    /** Name of the native library */
    private static final String LIBRARY_NAME = "forl0_native";

    /** Flag indicating whether the native library is available */
    private static volatile boolean nativeAvailable = false;

    /** Error message if native library loading failed */
    private static volatile String loadError = null;

    static {
        loadNativeLibrary();
    }

    private NativeL0Memory() {
        // Utility class, no instantiation
    }

    /**
     * Attempts to load the native library.
     */
    private static void loadNativeLibrary() {
        try {
            System.loadLibrary(LIBRARY_NAME);
            nativeAvailable = true;
            LOG.info("Successfully loaded native L0 memory library: {}", LIBRARY_NAME);
        } catch (UnsatisfiedLinkError e) {
            loadError = e.getMessage();
            nativeAvailable = false;
            LOG.warn("Failed to load native L0 memory library '{}': {}. " +
                     "L0Table will not be available.", LIBRARY_NAME, e.getMessage());
        }
    }

    /**
     * Checks if the native library is available.
     *
     * @return true if native methods can be used, false otherwise
     */
    public static boolean isAvailable() {
        return nativeAvailable;
    }

    /**
     * Gets the error message if native library loading failed.
     *
     * @return error message, or null if loading succeeded
     */
    public static String getLoadError() {
        return loadError;
    }

    // ==================== Native Methods ====================

    /**
     * Allocates a block of memory using native malloc.
     *
     * @param size Number of bytes to allocate
     * @return Native memory address, or 0 if allocation failed
     */
    public static native long malloc(long size);

    /**
     * Frees a previously allocated block of memory.
     *
     * @param address Native memory address returned by malloc
     */
    public static native void free(long address);

    /**
     * Allocates aligned memory using native posix_memalign or aligned_alloc.
     *
     * @param size Number of bytes to allocate
     * @param alignment Alignment boundary (must be power of 2)
     * @return Native memory address, or 0 if allocation failed
     */
    public static native long mallocAligned(long size, int alignment);

    /**
     * Copies data from a byte array to native memory.
     *
     * @param destAddress Destination native memory address
     * @param src Source byte array
     * @param srcOffset Offset in source array
     * @param length Number of bytes to copy
     */
    public static native void copyFromArray(long destAddress, byte[] src, int srcOffset, int length);

    /**
     * Copies data from native memory to a byte array.
     *
     * @param srcAddress Source native memory address
     * @param dest Destination byte array
     * @param destOffset Offset in destination array
     * @param length Number of bytes to copy
     */
    public static native void copyToArray(long srcAddress, byte[] dest, int destOffset, int length);

    /**
     * Gets a byte value from native memory.
     *
     * @param address Native memory address
     * @return Byte value at the address
     */
    public static native byte getByte(long address);

    /**
     * Sets a byte value in native memory.
     *
     * @param address Native memory address
     * @param value Byte value to set
     */
    public static native void putByte(long address, byte value);

    /**
     * Gets a short value from native memory (native byte order).
     *
     * @param address Native memory address
     * @return Short value at the address
     */
    public static native short getShort(long address);

    /**
     * Sets a short value in native memory (native byte order).
     *
     * @param address Native memory address
     * @param value Short value to set
     */
    public static native void putShort(long address, short value);

    /**
     * Gets an int value from native memory (native byte order).
     *
     * @param address Native memory address
     * @return Int value at the address
     */
    public static native int getInt(long address);

    /**
     * Sets an int value in native memory (native byte order).
     *
     * @param address Native memory address
     * @param value Int value to set
     */
    public static native void putInt(long address, int value);

    /**
     * Gets a long value from native memory (native byte order).
     *
     * @param address Native memory address
     * @return Long value at the address
     */
    public static native long getLong(long address);

    /**
     * Sets a long value in native memory (native byte order).
     *
     * @param address Native memory address
     * @param value Long value to set
     */
    public static native void putLong(long address, long value);

    /**
     * Sets a range of bytes in native memory to a specified value.
     *
     * @param address Native memory address
     * @param value Byte value to fill
     * @param length Number of bytes to set
     */
    public static native void memset(long address, byte value, long length);

    /**
     * Copies memory from one native address to another.
     *
     * @param destAddress Destination native memory address
     * @param srcAddress Source native memory address
     * @param length Number of bytes to copy
     */
    public static native void memcpy(long destAddress, long srcAddress, long length);
}
