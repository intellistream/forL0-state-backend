package org.apache.flink.runtime.state.heap.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

/**
 * JNI bridge for native L0 memory allocation.
 * This class provides native methods for allocating and freeing memory.
 *
 * <p>Two modes are supported:
 * <ul>
 *     <li><b>L0 Mode:</b> Uses L0 memory pool library (libl0mempool.so) on L0-enabled servers</li>
 *     <li><b>Simulation Mode:</b> Uses standard malloc/free for development/testing</li>
 * </ul>
 *
 * <p>The mode is automatically detected at runtime:
 * <ul>
 *     <li>If /dev/hisi_l0 exists and libl0mempool.so can be loaded -> L0 Mode</li>
 *     <li>Otherwise -> Simulation Mode</li>
 * </ul>
 *
 * <p>The native library must be loaded before using any native methods.
 * Use {@link #isAvailable()} to check if the native library is loaded.
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
     * First tries System.loadLibrary (for java.library.path),
     * then tries to extract from JAR resources.
     */
    private static void loadNativeLibrary() {
        // First try standard library path
        try {
            System.loadLibrary(LIBRARY_NAME);
            nativeAvailable = true;
            String modeDesc = getModeDescription();
            LOG.info("Successfully loaded native L0 memory library from system path: {} - {}", LIBRARY_NAME, modeDesc);
            // Print to stdout for visibility
            System.out.println("[ForL0] Native library loaded successfully");
            System.out.println("[ForL0] Running mode: " + modeDesc);
            System.out.println("[ForL0] L0 device: " + (isL0Mode() ? "/dev/hisi_l0 (AVAILABLE)" : "NOT AVAILABLE"));
            return;
        } catch (UnsatisfiedLinkError e) {
            LOG.debug("Native library not found in system path, trying to extract from JAR: {}", e.getMessage());
        }

        // Try to extract from JAR
        try {
            File extractedLib = extractNativeLibraryFromJar();
            if (extractedLib != null) {
                System.load(extractedLib.getAbsolutePath());
                nativeAvailable = true;
                String modeDesc = getModeDescription();
                LOG.info("Successfully loaded native L0 memory library from JAR: {} - {}", extractedLib.getAbsolutePath(), modeDesc);
                // Print to stdout for visibility
                System.out.println("[ForL0] Native library loaded from JAR: " + extractedLib.getAbsolutePath());
                System.out.println("[ForL0] Running mode: " + modeDesc);
                System.out.println("[ForL0] L0 device: " + (isL0Mode() ? "/dev/hisi_l0 (AVAILABLE)" : "NOT AVAILABLE"));
                return;
            }
        } catch (Exception e) {
            LOG.debug("Failed to extract native library from JAR: {}", e.getMessage());
        }

        loadError = "Native library '" + LIBRARY_NAME + "' not found in java.library.path or JAR resources";
        nativeAvailable = false;
        LOG.warn("Failed to load native L0 memory library '{}': {}. L0Table will not be available.", 
                 LIBRARY_NAME, loadError);
        System.err.println("[ForL0] WARNING: Failed to load native library - L0Table will not be available");
    }

    /**
     * Extracts the native library from JAR resources to a temporary file.
     *
     * @return the extracted library file, or null if not found
     */
    private static File extractNativeLibraryFromJar() throws IOException {
        String osName = System.getProperty("os.name").toLowerCase();
        String libExtension;
        String libPrefix = "lib";

        if (osName.contains("mac") || osName.contains("darwin")) {
            libExtension = ".dylib";
        } else if (osName.contains("win")) {
            libExtension = ".dll";
            libPrefix = "";
        } else {
            // Linux and others
            libExtension = ".so";
        }

        String libFileName = libPrefix + LIBRARY_NAME + libExtension;
        String resourcePath = "/native/" + libFileName;

        try (InputStream is = NativeL0Memory.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                LOG.debug("Native library resource not found: {}", resourcePath);
                return null;
            }

            // Create temp file with proper extension
            File tempDir = Files.createTempDirectory("forl0_native").toFile();
            tempDir.deleteOnExit();
            File tempFile = new File(tempDir, libFileName);
            tempFile.deleteOnExit();

            // Copy library to temp file
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }

            LOG.debug("Extracted native library to: {}", tempFile.getAbsolutePath());
            return tempFile;
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

    // -------------------- Mode Detection --------------------

    /** Mode constant: not initialized */
    public static final int MODE_NOT_INITIALIZED = 0;
    /** Mode constant: simulation mode (malloc/free) */
    public static final int MODE_SIMULATION = 1;
    /** Mode constant: L0 mode (libl0mempool.so) */
    public static final int MODE_L0 = 2;

    /**
     * Gets the current memory allocation mode.
     *
     * @return One of {@link #MODE_NOT_INITIALIZED}, {@link #MODE_SIMULATION}, or {@link #MODE_L0}
     */
    public static native int getMode();

    /**
     * Checks if running in L0 mode.
     *
     * @return true if using L0 memory pool library, false if using simulation mode
     */
    public static native boolean isL0Mode();

    /**
     * Sets the maximum capacity for L0 memory pool.
     * This must be called BEFORE any allocation if custom capacity is needed.
     * Once mode is initialized, capacity cannot be changed.
     *
     * @param capacity Maximum capacity in bytes
     */
    public static native void setMaxCapacity(long capacity);

    /**
     * Gets a human-readable description of the current mode.
     *
     * @return Mode description string
     */
    public static String getModeDescription() {
        if (!nativeAvailable) {
            return "Native library not available";
        }
        int mode = getMode();
        switch (mode) {
            case MODE_NOT_INITIALIZED:
                return "Not initialized";
            case MODE_SIMULATION:
                return "Simulation mode (malloc/free)";
            case MODE_L0:
                return "L0 mode (libl0mempool.so)";
            default:
                return "Unknown mode: " + mode;
        }
    }

    // -------------------- Memory Allocation --------------------

    /**
     * Allocates a block of memory.
     * In L0 mode, uses L0 memory pool; in simulation mode, uses malloc.
     *
     * @param size Number of bytes to allocate
     * @return Native memory address, or 0 if allocation failed
     */
    public static native long malloc(long size);

    /**
     * Frees a previously allocated block of memory.
     * In L0 mode, uses L0 memory pool; in simulation mode, uses free.
     *
     * @param address Native memory address returned by malloc
     */
    public static native void free(long address);

    /**
     * Allocates aligned memory.
     * In L0 mode, uses L0 memory pool (already aligned); in simulation mode, uses posix_memalign.
     *
     * @param size Number of bytes to allocate
     * @param alignment Alignment boundary (must be power of 2)
     * @return Native memory address, or 0 if allocation failed
     */
    public static native long mallocAligned(long size, int alignment);

    /**
     * Creates a raw memory pool with fixed size.
     * In L0 mode, uses mem_pool_create_raw(); in simulation mode, uses malloc.
     * 
     * @param name Pool name for debugging
     * @param size Pool size in bytes
     * @return Native pool address, or 0 if creation failed
     */
    public static native long createRawPool(String name, long size);

    /**
     * Releases a raw memory pool.
     * In L0 mode, uses mem_pool_release_raw(); in simulation mode, uses free.
     * 
     * @param poolAddress Native pool address returned by createRawPool
     */
    public static native void releaseRawPool(long poolAddress);

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
