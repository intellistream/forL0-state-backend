package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator over all state entries in the C++ ForL0 engine and heap PQ states.
 *
 * <p>Used by Flink's {@code FullSnapshotAsyncWriter} for canonical savepoints.
 * Iterates entries ordered by (keyGroup, kvStateId). For each KV state in each
 * key group, calls the per-state JNI method {@link NativeEngine#writeStateKeyGroupEntries}
 * to get entries in the C++ checkpoint binary format, then parses them to produce
 * composite keys and values.
 */
class ForL0KeyValueStateIterator implements KeyValueStateIterator {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** Binary format: 8 bytes fixed (INT64 or FLOAT64). */
    private static final int FMT_FIXED8 = 0;
    /** Binary format: 4-byte big-endian length prefix + data (BYTES). */
    private static final int FMT_LEN_PREFIXED = 1;
    /** Binary format: 4 bytes fixed (INT32). */
    private static final int FMT_FIXED4 = 2;

    // ---- State descriptors ----

    private static final class StateDesc {
        final int kvStateId;
        final boolean isKV;
        // KV fields
        final long nativeHandle;
        final int keyFormat;   // FMT_FIXED8 or FMT_LEN_PREFIXED
        final int valueFormat; // FMT_FIXED8 or FMT_LEN_PREFIXED
        final boolean voidNamespace;
        // PQ fields
        final HeapPriorityQueueStateSnapshot<?> pqSnapshot;
        final TypeSerializer<?> elementSerializer;

        /** KV state constructor. */
        StateDesc(int kvStateId, long handle, int keyFmt, int valFmt, boolean voidNs) {
            this.kvStateId = kvStateId;
            this.isKV = true;
            this.nativeHandle = handle;
            this.keyFormat = keyFmt;
            this.valueFormat = valFmt;
            this.voidNamespace = voidNs;
            this.pqSnapshot = null;
            this.elementSerializer = null;
        }

        /** PQ state constructor. */
        StateDesc(int kvStateId, HeapPriorityQueueStateSnapshot<?> snapshot,
                  TypeSerializer<?> elemSer) {
            this.kvStateId = kvStateId;
            this.isKV = false;
            this.nativeHandle = 0;
            this.keyFormat = 0;
            this.valueFormat = 0;
            this.voidNamespace = false;
            this.pqSnapshot = snapshot;
            this.elementSerializer = elemSer;
        }
    }

    // ---- Iteration state ----

    private final StateDesc[] states;
    private final int keyGroupPrefixBytes;
    private final int endKeyGroup; // exclusive

    private int currentKeyGroup;
    private int stateOrdinal;

    // KV entry iteration for current (state, keyGroup)
    private ByteBuffer kvBuf;
    private int kvTotal;
    private int kvIdx;

    // PQ entry iteration
    private Iterator<?> pqIter;

    // Current output
    private byte[] currentKey;
    private byte[] currentValue;
    private boolean isValid;
    private boolean newKeyGroup;
    private boolean newKVState;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(128);

    // ---- Construction ----

    @SuppressWarnings("unchecked")
    ForL0KeyValueStateIterator(
            @Nonnull KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            @Nonnull TypeSerializer<?> keySerializer,
            @Nonnull Map<StateUID, Integer> stateNamesToId,
            @Nonnull Map<String, Long> nativeStateHandles,
            @Nonnull Map<String, RegisteredKeyValueStateBackendMetaInfo<?, ?>> registeredMetaInfos,
            @Nonnull Map<StateUID, StateSnapshot> pqSnapshots) throws IOException {

        this.keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        this.endKeyGroup = keyGroupRange.getEndKeyGroup() + 1;

        int keyTypeId = TypeAnalyzer.getTypeId(keySerializer);

        // Build state descriptors from all registered KV and PQ states
        List<StateDesc> list = new ArrayList<>();
        for (Map.Entry<StateUID, Integer> e : stateNamesToId.entrySet()) {
            StateUID uid = e.getKey();
            int kvStateId = e.getValue();

            if (uid.getStateType() == StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                String name = uid.getStateName();
                Long handle = nativeStateHandles.get(name);
                RegisteredKeyValueStateBackendMetaInfo<?, ?> meta = registeredMetaInfos.get(name);
                if (handle == null || meta == null) continue;

                boolean isValue = meta.getStateType() == StateDescriptor.Type.VALUE;
                int valTypeId = TypeAnalyzer.getTypeId(meta.getStateSerializer());

                // Determine stored binary format — must match C++ registration rules
                int keyFmt, valFmt;
                if (isValue) {
                    if (keyTypeId == TypeAnalyzer.TYPE_INT64
                            && (valTypeId == TypeAnalyzer.TYPE_INT64
                            || valTypeId == TypeAnalyzer.TYPE_INT32)) {
                        keyFmt = FMT_FIXED8;
                        valFmt = FMT_FIXED8;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_INT64
                            && valTypeId == TypeAnalyzer.TYPE_FLOAT64) {
                        keyFmt = FMT_FIXED8;
                        valFmt = FMT_FIXED8;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_INT64) {
                        keyFmt = FMT_FIXED8;
                        valFmt = FMT_LEN_PREFIXED;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_INT32
                            && (valTypeId == TypeAnalyzer.TYPE_INT64
                            || valTypeId == TypeAnalyzer.TYPE_INT32)) {
                        keyFmt = FMT_FIXED4;
                        valFmt = FMT_FIXED8;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_INT32
                            && valTypeId == TypeAnalyzer.TYPE_FLOAT64) {
                        keyFmt = FMT_FIXED4;
                        valFmt = FMT_FIXED8;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_INT32) {
                        keyFmt = FMT_FIXED4;
                        valFmt = FMT_LEN_PREFIXED;
                    } else if (keyTypeId == TypeAnalyzer.TYPE_FIXED_ROW) {
                        keyFmt = FMT_LEN_PREFIXED;
                        if (valTypeId == TypeAnalyzer.TYPE_INT64
                                || valTypeId == TypeAnalyzer.TYPE_INT32
                                || valTypeId == TypeAnalyzer.TYPE_FLOAT64) {
                            valFmt = FMT_FIXED8;
                        } else {
                            valFmt = FMT_LEN_PREFIXED;
                        }
                    } else {
                        keyFmt = FMT_LEN_PREFIXED;
                        valFmt = FMT_LEN_PREFIXED;
                    }
                } else {
                    // LIST, MAP, REDUCING, AGGREGATING
                    if (keyTypeId == TypeAnalyzer.TYPE_INT64) {
                        keyFmt = FMT_FIXED8;
                    } else {
                        keyFmt = FMT_LEN_PREFIXED;
                    }
                    // REDUCING with INT64 key + INT64 value stores <int64_t, int64_t>
                    boolean isReducing = (meta.getStateType() == StateDescriptor.Type.REDUCING);
                    if (isReducing && keyTypeId == TypeAnalyzer.TYPE_INT64
                            && valTypeId == TypeAnalyzer.TYPE_INT64) {
                        valFmt = FMT_FIXED8;
                    } else {
                        valFmt = FMT_LEN_PREFIXED;
                    }
                }

                boolean voidNs = meta.getNamespaceSerializer() instanceof VoidNamespaceSerializer;
                list.add(new StateDesc(kvStateId, handle, keyFmt, valFmt, voidNs));
            } else {
                // Priority queue state
                StateSnapshot snapshot = pqSnapshots.get(uid);
                if (snapshot instanceof HeapPriorityQueueStateSnapshot) {
                    HeapPriorityQueueStateSnapshot<?> pqSnap =
                            (HeapPriorityQueueStateSnapshot<?>) snapshot;
                    TypeSerializer<?> elemSer =
                            new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                    pqSnap.getMetaInfoSnapshot()).getElementSerializer();
                    list.add(new StateDesc(kvStateId, pqSnap, elemSer));
                }
            }
        }
        list.sort(Comparator.comparingInt(d -> d.kvStateId));
        this.states = list.toArray(new StateDesc[0]);

        // Position at first valid entry
        this.currentKeyGroup = keyGroupRange.getStartKeyGroup();
        this.stateOrdinal = -1;
        this.isValid = false;
        this.newKeyGroup = true;
        this.newKVState = true;

        if (currentKeyGroup < endKeyGroup && states.length > 0) {
            advanceToNextEntry();
        }
    }

    // ---- KeyValueStateIterator interface ----

    @Override public boolean isValid() { return isValid; }
    @Override public boolean isNewKeyGroup() { return newKeyGroup; }
    @Override public boolean isNewKeyValueState() { return newKVState; }
    @Override public int keyGroup() { return currentKeyGroup; }
    @Override public int kvStateId() { return states[stateOrdinal].kvStateId; }
    @Override public byte[] key() { return currentKey; }
    @Override public byte[] value() { return currentValue; }

    @Override
    public void next() throws IOException {
        newKeyGroup = false;
        newKVState = false;
        advanceToNextEntry();
    }

    @Override
    public void close() {
        // No resources to release
    }

    // ---- Iteration logic ----

    private void advanceToNextEntry() throws IOException {
        while (true) {
            // Try next entry from current state + keyGroup
            if (stateOrdinal >= 0 && stateOrdinal < states.length) {
                StateDesc desc = states[stateOrdinal];
                if (desc.isKV ? tryNextKVEntry(desc) : tryNextPQEntry(desc)) {
                    isValid = true;
                    return;
                }
            }

            // Move to next state within current key group
            stateOrdinal++;
            if (stateOrdinal < states.length) {
                newKVState = true;
                loadState();
                continue;
            }

            // Move to next key group
            currentKeyGroup++;
            if (currentKeyGroup < endKeyGroup) {
                stateOrdinal = 0;
                newKeyGroup = true;
                newKVState = true;
                loadState();
                continue;
            }

            // Exhausted all key groups
            isValid = false;
            return;
        }
    }

    /** Load data for states[stateOrdinal] in currentKeyGroup. */
    private void loadState() {
        StateDesc desc = states[stateOrdinal];
        if (desc.isKV) {
            byte[] data = NativeEngine.writeStateKeyGroupEntries(
                    desc.nativeHandle, currentKeyGroup);
            if (data != null && data.length >= 4) {
                kvBuf = ByteBuffer.wrap(data);
                kvTotal = kvBuf.getInt();
                kvIdx = 0;
            } else {
                kvBuf = null;
                kvTotal = 0;
                kvIdx = 0;
            }
            pqIter = null;
        } else {
            pqIter = desc.pqSnapshot.getIteratorForKeyGroup(currentKeyGroup);
            kvBuf = null;
            kvTotal = 0;
            kvIdx = 0;
        }
    }

    // ---- KV entry parsing ----

    /**
     * Try to read the next KV entry from the C++ binary data buffer.
     *
     * <p>Binary format per entry (written by CheckpointStateWriter):
     * <ul>
     *   <li>Namespace: 1 byte (VoidNamespace=0x00) or 4-byte len + data</li>
     *   <li>Key: 8 bytes (INT64) or 4-byte len + data (BYTES)</li>
     *   <li>Value: 8 bytes (INT64/FLOAT64) or 4-byte len + data (BYTES)</li>
     * </ul>
     *
     * <p>Composite key output: [keyGroupPrefix] + [keyBytes] + [namespaceBytes]
     */
    private boolean tryNextKVEntry(StateDesc desc) throws IOException {
        if (kvIdx >= kvTotal) return false;

        // Read namespace bytes
        byte[] nsBytes;
        if (desc.voidNamespace) {
            nsBytes = new byte[]{kvBuf.get()};
        } else {
            int nsLen = kvBuf.getInt();
            nsBytes = new byte[nsLen];
            kvBuf.get(nsBytes);
        }

        // Read key bytes (strip length prefix for BYTES format)
        byte[] keyBytes = readField(desc.keyFormat);

        // Read value bytes (strip length prefix for BYTES format)
        byte[] valBytes = readField(desc.valueFormat);

        // Build composite key: [keyGroupPrefix][keyBytes][nsBytes]
        keyOut.clear();
        CompositeKeySerializationUtils.writeKeyGroup(
                currentKeyGroup, keyGroupPrefixBytes, keyOut);
        keyOut.write(keyBytes);
        keyOut.write(nsBytes);
        currentKey = keyOut.getCopyOfBuffer();
        currentValue = valBytes;

        kvIdx++;
        return true;
    }

    private byte[] readField(int format) {
        if (format == FMT_FIXED8) {
            byte[] buf = new byte[8];
            kvBuf.get(buf);
            return buf;
        } else if (format == FMT_FIXED4) {
            byte[] buf = new byte[4];
            kvBuf.get(buf);
            return buf;
        } else {
            int len = kvBuf.getInt();
            byte[] buf = new byte[len];
            kvBuf.get(buf);
            return buf;
        }
    }

    // ---- PQ entry handling ----

    @SuppressWarnings("unchecked")
    private boolean tryNextPQEntry(StateDesc desc) throws IOException {
        if (pqIter == null || !pqIter.hasNext()) return false;

        Object element = pqIter.next();
        keyOut.clear();
        CompositeKeySerializationUtils.writeKeyGroup(
                currentKeyGroup, keyGroupPrefixBytes, keyOut);
        ((TypeSerializer<Object>) desc.elementSerializer).serialize(element, keyOut);
        currentKey = keyOut.getCopyOfBuffer();
        currentValue = EMPTY_BYTE_ARRAY;
        return true;
    }
}
