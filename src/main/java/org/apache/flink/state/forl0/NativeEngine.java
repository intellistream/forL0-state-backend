package org.apache.flink.state.forl0;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * JNI bridge to the C++ ForL0 state engine.
 *
 * <p>All native methods are declared here. The native library (libforl0_engine.so/dylib)
 * is loaded once at class initialization. Each method maps 1:1 to a C++ function
 * in the JNI bridge layer.
 *
 * <p>Handle convention: C++ objects are referenced by {@code long} handles
 * (pointer addresses cast to jlong). The Java thin shell holds these handles
 * and passes them through on every operation.
 */
public final class NativeEngine {

    private static final Logger LOG = LoggerFactory.getLogger(NativeEngine.class);

    private static volatile boolean loaded = false;
    private static volatile String loadError = null;

    static {
        loadNativeLibrary();
    }

    private NativeEngine() {}

    // ========================================================================
    //  Library loading
    // ========================================================================

    private static void loadNativeLibrary() {
        if (loaded) return;

        // Strategy 1: try System.loadLibrary (honors java.library.path)
        try {
            System.loadLibrary("forl0_engine");
            loaded = true;
            LOG.info("[ForL0] Native engine loaded via java.library.path");
            return;
        } catch (UnsatisfiedLinkError e) {
            LOG.debug("[ForL0] loadLibrary failed, trying bundled resource: {}", e.getMessage());
        }

        // Strategy 2: extract from JAR resource to temp dir and load
        String osName = System.getProperty("os.name", "").toLowerCase();
        String libName = osName.contains("mac") || osName.contains("darwin")
                ? "libforl0_engine.dylib"
                : "libforl0_engine.so";
        try (InputStream in = NativeEngine.class.getResourceAsStream("/native/" + libName)) {
            if (in == null) {
                loadError = "Native library resource /native/" + libName + " not found in JAR";
                LOG.error("[ForL0] {}", loadError);
                return;
            }
            File tmpDir = Files.createTempDirectory("forl0_native_").toFile();
            tmpDir.deleteOnExit();
            File tmpLib = new File(tmpDir, libName);
            tmpLib.deleteOnExit();
            Files.copy(in, tmpLib.toPath(), StandardCopyOption.REPLACE_EXISTING);
            System.load(tmpLib.getAbsolutePath());
            loaded = true;
            LOG.info("[ForL0] Native engine loaded from bundled resource: {}", tmpLib.getAbsolutePath());
        } catch (IOException | UnsatisfiedLinkError e) {
            loadError = e.getMessage();
            LOG.error("[ForL0] Failed to load native engine: {}", loadError);
        }
    }

    public static boolean isLoaded() {
        return loaded;
    }

    public static void ensureLoaded() {
        if (!loaded) {
            throw new IllegalStateException(
                    "[ForL0] Native engine not available: " + loadError);
        }
    }

    // ========================================================================
    //  StateEngine lifecycle
    // ========================================================================

    /** Create a new StateEngine. Returns native handle. */
    public static native long createEngine(int startKeyGroup, int numKeyGroups, int totalKeyGroups);

    /** Destroy a StateEngine and free all native memory. */
    public static native void destroyEngine(long engineHandle);

    // ========================================================================
    //  State registration
    // ========================================================================

    /**
     * Register a state in the engine. Returns a state handle.
     *
     * @param engineHandle  the engine handle
     * @param stateName     state descriptor name
     * @param stateType     0=Value, 1=List, 2=Map, 3=Reducing, 4=Aggregating
     * @param keyTypeId     TypeId of the key type
     * @param valueTypeId   TypeId of the value type
     * @param nsTypeId      TypeId of the namespace type (20=VoidNamespace)
     * @param typeDescriptor serialized TypeLayout descriptor (from TypeAnalyzer)
     * @return state handle (long)
     */
    public static native long registerState(long engineHandle, String stateName,
                                            int stateType,
                                            int keyTypeId, int valueTypeId, int nsTypeId,
                                            byte[] typeDescriptor);

    // ========================================================================
    //  ValueState operations — primitive key specializations
    // ========================================================================

    // --- long key + long value (VoidNamespace) ---

    public static native long valueGetLongLong(long stateHandle, long key, int keyGroup);

    public static native void valuePutLongLong(long stateHandle, long key, int keyGroup, long value);

    public static native void valueClearLong(long stateHandle, long key, int keyGroup);

    // --- long key + int value (VoidNamespace) ---

    public static native int valueGetLongInt(long stateHandle, long key, int keyGroup);

    public static native void valuePutLongInt(long stateHandle, long key, int keyGroup, int value);

    // --- long key + double value (VoidNamespace) ---

    public static native double valueGetLongDouble(long stateHandle, long key, int keyGroup);

    public static native void valuePutLongDouble(long stateHandle, long key, int keyGroup, double value);

    // --- long key + String value (VoidNamespace) ---

    public static native byte[] valueGetLongString(long stateHandle, long key, int keyGroup);

    public static native void valuePutLongString(long stateHandle, long key, int keyGroup, byte[] value);

    // --- int key + long value (VoidNamespace) ---

    public static native long valueGetIntLong(long stateHandle, int key, int keyGroup);

    public static native void valuePutIntLong(long stateHandle, int key, int keyGroup, long value);

    // --- int key + double value (VoidNamespace) ---

    public static native double valueGetIntDouble(long stateHandle, int key, int keyGroup);

    public static native void valuePutIntDouble(long stateHandle, int key, int keyGroup, double value);

    // --- int key + String value (VoidNamespace) ---

    public static native byte[] valueGetIntString(long stateHandle, int key, int keyGroup);

    public static native void valuePutIntString(long stateHandle, int key, int keyGroup, byte[] value);

    // --- int key existence check and clear ---

    public static native boolean valueContainsInt(long stateHandle, int key, int keyGroup);

    public static native void valueClearInt(long stateHandle, int key, int keyGroup);

    // --- Generic: key and value as serialized byte[] (fallback for unspecialized types) ---

    public static native byte[] valueGetGeneric(long stateHandle, byte[] key, int keyGroup);

    public static native void valuePutGeneric(long stateHandle, byte[] key, int keyGroup, byte[] value);

    public static native void valueClearGeneric(long stateHandle, byte[] key, int keyGroup);

    // --- Zero-copy pointer return: writes [nativeAddress, size] into out array ---
    // Returns true if value found, false if not. Caller wraps as off-heap MemorySegment.

    public static native boolean valueGetLongStringPtr(long stateHandle, long key, int keyGroup, long[] out);

    public static native boolean valueGetGenericPtr(long stateHandle, byte[] key, int keyGroup, long[] out);

    // --- ValueState existence check ---

    public static native boolean valueContains(long stateHandle, long key, int keyGroup);

    public static native boolean valueContainsGeneric(long stateHandle, byte[] key, int keyGroup);

    // ========================================================================
    //  ListState operations
    // ========================================================================

    /** Get list elements as a serialized byte[] (Flink list format). */
    public static native byte[] listGet(long stateHandle, long key, int keyGroup);

    public static native byte[] listGetGeneric(long stateHandle, byte[] key, int keyGroup);

    /** Add a single element (int64 key + serialized element bytes). */
    public static native void listAdd(long stateHandle, long key, int keyGroup, byte[] element);

    /** Add a single element (int64 key + long element). */
    public static native void listAddLong(long stateHandle, long key, int keyGroup, long element);

    public static native void listAddGeneric(long stateHandle, byte[] key, int keyGroup, byte[] element);

    /** Get list elements as long[] (int64 key + long elements). Returns null if absent. */
    public static native long[] listGetLongElements(long stateHandle, long key, int keyGroup);

    /** Replace entire list with long[] (int64 key + long elements). */
    public static native void listUpdateLongElements(long stateHandle, long key, int keyGroup, long[] elements);

    /** Add all long elements (int64 key + long elements). */
    public static native void listAddAllLongElements(long stateHandle, long key, int keyGroup, long[] elements);

    /** Replace entire list. */
    public static native void listUpdate(long stateHandle, long key, int keyGroup, byte[] serializedList);

    public static native void listUpdateGeneric(long stateHandle, byte[] key, int keyGroup, byte[] serializedList);

    /** Add all elements. */
    public static native void listAddAll(long stateHandle, long key, int keyGroup, byte[] serializedElements);

    public static native void listAddAllGeneric(long stateHandle, byte[] key, int keyGroup, byte[] serializedElements);

    /** Clear list. */
    public static native void listClear(long stateHandle, long key, int keyGroup);

    public static native void listClearGeneric(long stateHandle, byte[] key, int keyGroup);

    /** Merge namespaces (window trigger). */
    public static native void listMergeNamespaces(long stateHandle, long key, int keyGroup,
                                                  long targetNamespace, long[] sourceNamespaces);

    // ========================================================================
    //  MapState operations
    // ========================================================================

    public static native byte[] mapGet(long stateHandle, long key, int keyGroup, byte[] userKey);

    public static native byte[] mapGetGeneric(long stateHandle, byte[] key, int keyGroup, byte[] userKey);

    public static native void mapPut(long stateHandle, long key, int keyGroup, byte[] userKey, byte[] userValue);

    public static native void mapPutGeneric(long stateHandle, byte[] key, int keyGroup, byte[] userKey, byte[] userValue);

    public static native void mapRemove(long stateHandle, long key, int keyGroup, byte[] userKey);

    public static native void mapRemoveGeneric(long stateHandle, byte[] key, int keyGroup, byte[] userKey);

    public static native boolean mapContains(long stateHandle, long key, int keyGroup, byte[] userKey);

    public static native boolean mapContainsGeneric(long stateHandle, byte[] key, int keyGroup, byte[] userKey);

    /** Get all entries as serialized byte[] (entry count + key-value pairs). */
    public static native byte[] mapEntries(long stateHandle, long key, int keyGroup);

    public static native byte[] mapEntriesGeneric(long stateHandle, byte[] key, int keyGroup);

    public static native byte[] mapKeys(long stateHandle, long key, int keyGroup);

    public static native byte[] mapValues(long stateHandle, long key, int keyGroup);

    public static native boolean mapIsEmpty(long stateHandle, long key, int keyGroup);

    public static native boolean mapIsEmptyGeneric(long stateHandle, byte[] key, int keyGroup);

    public static native void mapClear(long stateHandle, long key, int keyGroup);

    public static native void mapClearGeneric(long stateHandle, byte[] key, int keyGroup);

    // --- MapState Long UK/UV zero-serialization paths ---
    /** Get UV(long) by Long key + Long UK. Returns Long.MIN_VALUE if absent. */
    public static native long mapGetLongLong(long stateHandle, long key, int keyGroup, long userKey);
    /** Check existence by Long key + Long UK. */
    public static native boolean mapContainsLongLong(long stateHandle, long key, int keyGroup, long userKey);
    /** Put Long UK + Long UV. */
    public static native void mapPutLongLong(long stateHandle, long key, int keyGroup, long userKey, long userValue);
    /** Remove by Long UK. */
    public static native void mapRemoveLongLong(long stateHandle, long key, int keyGroup, long userKey);
    /** Get all entries as interleaved long[] [uk0,uv0,uk1,uv1,...]. Returns null if empty. */
    public static native long[] mapEntriesLongLong(long stateHandle, long key, int keyGroup);

    // ========================================================================
    //  ReducingState / AggregatingState operations
    // ========================================================================

    // For built-in aggregations (SUM, MIN, MAX), C++ can do it in-place.
    // builtinAggType: 0=SUM, 1=MIN, 2=MAX, -1=user-defined (requires callback)

    public static native long reduceGetLong(long stateHandle, long key, int keyGroup);

    public static native void reduceAddLong(long stateHandle, long key, int keyGroup,
                                            long value, int builtinAggType);

    public static native byte[] reduceGetGeneric(long stateHandle, byte[] key, int keyGroup);

    public static native void reduceAddGeneric(long stateHandle, byte[] key, int keyGroup,
                                               byte[] value, int builtinAggType);

    public static native void reduceClear(long stateHandle, long key, int keyGroup);

    // Aggregating state: accumulator get/add
    public static native byte[] aggGetGeneric(long stateHandle, byte[] key, int keyGroup);

    public static native void aggAddGeneric(long stateHandle, byte[] key, int keyGroup,
                                            byte[] accumulator);

    // ========================================================================
    //  Checkpoint operations
    // ========================================================================

    /** Prepare for snapshot — returns snapshot version. */
    public static native long prepareSnapshot(long engineHandle);

    /**
     * Write a key group's state data into the provided buffer.
     * Returns a byte[] containing the serialized data for this key group.
     */
    public static native byte[] writeKeyGroupData(long engineHandle, int keyGroupId);

    /**
     * Read a key group's data from a byte buffer during restore.
     */
    public static native void readKeyGroupData(long engineHandle, int keyGroupId, byte[] data);

    /**
     * Write entries for a single state in a single key group.
     * Returns byte[] with format: [count(4)][entries...] where each entry is
     * [namespace_bytes][key_bytes][value_bytes] in Flink serialization format.
     * Returns null if no entries exist.
     */
    public static native byte[] writeStateKeyGroupEntries(long stateHandle, int keyGroupId);

    /**
     * Read entries for a single state in a single key group from canonical savepoint data.
     * Data format: [count(4)][entries...] where each entry matches the checkpoint binary format
     * for this state's stored types.
     */
    public static native void readStateKeyGroupEntries(long stateHandle, int keyGroupId, byte[] data);

    // ========================================================================
    //  Utility methods
    // ========================================================================

    /** Total number of state entries across all states. */
    public static native long totalEntries(long engineHandle);

    /** Number of entries in a specific state. */
    public static native long stateEntries(long stateHandle);

    /**
     * Collect all keys of a given state as serialized bytes.
     * Returns a byte[] in format: [keyCount(int)] + for each key: [keyBytes].
     * For long keys: each key is 8 bytes big-endian.
     * For string/bytes keys: each key is [length(int)] + [bytes].
     */
    public static native byte[] getStateKeys(long stateHandle);

    // ========================================================================
    //  FixedLengthRow operations (RowData zero-serialization path)
    // ========================================================================

    // --- FixedLengthRow key + long value (VoidNamespace, most common SQL aggregation) ---

    public static native long valueGetFixedRowLong(long stateHandle, long[] keyFields, int keyGroup);

    public static native void valuePutFixedRowLong(long stateHandle, long[] keyFields, int keyGroup, long value);

    public static native boolean valueContainsFixedRow(long stateHandle, long[] keyFields, int keyGroup);

    public static native void valueClearFixedRow(long stateHandle, long[] keyFields, int keyGroup);

    // --- FixedLengthRow key + double value (VoidNamespace) ---

    public static native double valueGetFixedRowDouble(long stateHandle, long[] keyFields, int keyGroup);

    public static native void valuePutFixedRowDouble(long stateHandle, long[] keyFields, int keyGroup, double value);

    // --- FixedLengthRow key + generic value (VoidNamespace) ---

    public static native byte[] valueGetFixedRowGeneric(long stateHandle, long[] keyFields, int keyGroup);

    public static native void valuePutFixedRowGeneric(long stateHandle, long[] keyFields, int keyGroup, byte[] value);
}
