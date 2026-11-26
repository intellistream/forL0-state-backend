package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentBridge;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * ForL0 implementation of {@link InternalMapState}.
 *
 * <p>JNI thin shell — all state storage is in the C++ engine.
 * Each method forwards to a single per-entry {@link NativeEngine} JNI call.
 * The inner map is maintained as a native C++ unordered_map in the SwissTable slot.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <UK> The type of user map key
 * @param <UV> The type of user map value
 */
public class ForL0MapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {

    private final long stateHandle;
    private final boolean timeWindowMap;
    private final boolean rowKeyIntMap;
    private final boolean rowKeyMap;
    private final long readResultAddress;
    private final NamespaceReadIndex rootReadIndex;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<Map<UK, UV>> valueSerializer;
    private TypeSerializer<UK> userKeySerializer;
    private TypeSerializer<UV> userValueSerializer;
    private TypeSerializer<?> listRowElementSerializer;
    private int listRowArity;
    private N currentNamespace;

    private final int keyTypeId;
    private final int ukTypeId;
    private final int uvTypeId;
    private final boolean voidNamespace;
    private final boolean isTimeWindowNs;
    private final boolean isRowDataKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataUK;
    private final RowDataKeyAccessor rowDataUKAccessor;
    private final boolean isRowDataUV;
    private final RowDataKeyAccessor rowDataUVAccessor;

    private enum MapStrategy {
        LONG_LONG_VOID, LONG_BYTES_VOID, BYTES_LONG_VOID,
        LONG_MAP_VOID, LONG_MAP_TW, GENERIC
    }
    private final MapStrategy mapStrategy;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer ukOut = new DataOutputSerializer(64);
    private final DataOutputSerializer uvOut = new DataOutputSerializer(128);

    ForL0MapState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer) {
        this.stateHandle = stateHandle;
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.userKeySerializer = userKeySerializer;
        configureUserValueSerializer(userValueSerializer);
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        // Use TypeAnalyzer (same as C++ registration) to detect INT64 — handles both
        // raw LongSerializer and RowData-wrapped BIGINT.
        this.ukTypeId = effectiveInt64TypeId(userKeySerializer);
        this.uvTypeId = effectiveInt64TypeId(userValueSerializer);
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);
        this.rootReadIndex = voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64
                ? NamespaceReadIndex.forMap(stateHandle, keyContext.keyGroupRange.getStartKeyGroup()) : null;
        this.timeWindowMap = voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64
                && TypeAnalyzer.isAnyTimeWindowSerializer(userKeySerializer)
                && TypeAnalyzer.isAnyTimeWindowSerializer(userValueSerializer);
        this.readResultAddress = NativeEngine.stateReadResultAddress(stateHandle);
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataUK = userKeySerializer != null && TypeAnalyzer.isRowDataSerializer(userKeySerializer);
        this.rowDataUKAccessor = isRowDataUK ? TypeAnalyzer.createRowDataKeyAccessor(userKeySerializer) : null;
        this.isRowDataUV = userValueSerializer != null && TypeAnalyzer.isRowDataSerializer(userValueSerializer);
        this.rowDataUVAccessor = isRowDataUV ? TypeAnalyzer.createRowDataKeyAccessor(userValueSerializer) : null;
        this.rowKeyMap = voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && isRowDataUK
                && ukTypeId != TypeAnalyzer.TYPE_INT64 && uvTypeId != TypeAnalyzer.TYPE_INT64
                && !isRowDataUV && userValueSerializer.getLength() >= 0;
        this.rowKeyIntMap = rowKeyMap
                && userValueSerializer instanceof org.apache.flink.api.common.typeutils.base.IntSerializer;

        if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            if (ukTypeId == TypeAnalyzer.TYPE_INT64 && uvTypeId == TypeAnalyzer.TYPE_INT64) {
                this.mapStrategy = MapStrategy.LONG_LONG_VOID;
            } else if (ukTypeId == TypeAnalyzer.TYPE_INT64) {
                this.mapStrategy = MapStrategy.LONG_BYTES_VOID;
            } else if (uvTypeId == TypeAnalyzer.TYPE_INT64) {
                this.mapStrategy = MapStrategy.BYTES_LONG_VOID;
            } else {
                this.mapStrategy = MapStrategy.LONG_MAP_VOID;
            }
        } else if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.mapStrategy = MapStrategy.LONG_MAP_TW;
        } else {
            this.mapStrategy = MapStrategy.GENERIC;
        }
    }

    /**
     * Return TYPE_INT64 if the serializer is recognized as int64 by TypeAnalyzer
     * (primitive Long OR RowData-wrapped single BIGINT), -1 otherwise.
     * This MUST match the logic used during C++ state registration.
     */
    private static int effectiveInt64TypeId(TypeSerializer<?> s) {
        if (s == null) return -1;
        int id = TypeAnalyzer.getTypeId(s);
        return (id == TypeAnalyzer.TYPE_INT64) ? TypeAnalyzer.TYPE_INT64 : -1;
    }

    /** Resolve user-key to long — works for both primitive Long and RowData[BIGINT]. */
    private long resolveUKAsLong(UK userKey) {
        if (isRowDataUK) {
            return rowDataUKAccessor.extractSingleLong(userKey);
        }
        return (Long) userKey;
    }

    /** Resolve user-value to long — works for both primitive Long and RowData[BIGINT]. */
    private long resolveUVAsLong(UV userValue) {
        if (isRowDataUV) {
            return rowDataUVAccessor.extractSingleLong(userValue);
        }
        return (Long) userValue;
    }

    /** Wrap a long value back to the UK type (Long or RowData). */
    @SuppressWarnings("unchecked")
    private UK wrapLongAsUK(long value) {
        if (isRowDataUK) {
            return (UK) TypeAnalyzer.toBinaryRow(userKeySerializer,
                    rowDataUKAccessor.reconstructFromLong(value)).copy();
        }
        return (UK) Long.valueOf(value);
    }

    /** Wrap a long value back to the UV type (Long or RowData). */
    @SuppressWarnings("unchecked")
    private UV wrapLongAsUV(long value) {
        if (isRowDataUV) {
            return (UV) rowDataUVAccessor.reconstructFromLong(value);
        }
        return (UV) Long.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private UV getWindowMapping(UK userKey) {
        long key = resolveKeyAsLong(keyContext.currentKey);
        int group = keyContext.currentKeyGroupIndex;
        if (userKey == null && absentRoot(key)) return null;
        long start = TypeAnalyzer.windowStart(userKey), end = TypeAnalyzer.windowEnd(userKey);
        int cached = fixedMapEntry(group, key, start, end, true);
        if (cached == NamespaceReadIndex.ABSENT) return null;
        if (cached == NamespaceReadIndex.NUMBER) {
            return (UV) TypeAnalyzer.timeWindow(userValueSerializer, rootReadIndex.result, rootReadIndex.secondResult);
        }
        if (!NativeEngine.mapGetTimeWindow(stateHandle, key, group, start, end)) {
            return null;
        }
        return (UV) TypeAnalyzer.timeWindow(userValueSerializer,
                MemorySegmentBridge.readNativeLong(readResultAddress),
                MemorySegmentBridge.readNativeLong(readResultAddress + 8));
    }

    @Override
    public UV get(UK userKey) {
        if (rowKeyIntMap) return getRowKeyInt(userKey);
        if (timeWindowMap) return getWindowMapping(userKey);
        if (rowKeyMap) return getRowKeyBytes(userKey);
        return getOtherStrategies(userKey);
    }

    @SuppressWarnings("unchecked")
    private UV getRowKeyInt(UK userKey) {
        long key = resolveKeyAsLong(keyContext.currentKey);
        int group = keyContext.currentKeyGroupIndex;
        int cached = userKey != null && rootReadIndex != null ? rootReadIndex.readRowMap(group, key) : NamespaceReadIndex.UNKNOWN;
        if (cached == NamespaceReadIndex.ABSENT) return null;
        BinaryRowData row = contiguousUserKey(userKey);
        MemorySegment segment = row.getSegments()[0];
        int size = row.getSizeInBytes(), offset = row.getOffset();
        if (cached == NamespaceReadIndex.MAP) {
            return rootReadIndex.readRowMapValue(segment, offset, size) == NamespaceReadIndex.NUMBER
                    ? (UV) Integer.valueOf((int) rootReadIndex.result) : null;
        }
        long encoded = segment.isOffHeap()
                ? NativeEngine.mapGetRowKeyIntAddress(stateHandle, key, group, row, segment.getAddress() + offset, size)
                : NativeEngine.mapGetRowKeyInt(stateHandle, key, group, segment.getArray(), offset, size);
        return encoded == 0 ? null : (UV) Integer.valueOf((int) encoded);
    }

    private UV getRowKeyBytes(UK userKey) {
        try {
            long key = resolveKeyAsLong(keyContext.currentKey);
            if (userKey != null && absentRoot(key)) return null;
            BinaryRowData row = contiguousUserKey(userKey);
            MemorySegment segment = row.getSegments()[0];
            int group = keyContext.currentKeyGroupIndex, size = row.getSizeInBytes(), offset = row.getOffset();
            byte[] bytes = segment.isOffHeap()
                    ? NativeEngine.mapGetRowKeyBytesAddress(stateHandle, key, group, row, segment.getAddress() + offset, size)
                    : NativeEngine.mapGetRowKeyBytes(stateHandle, key, group, segment.getArray(), offset, size);
            return bytes == null ? null : deserializeUserValue(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get from MapState", e);
        }
    }

    private UV getWithTimeWindowNamespace(UK userKey) throws IOException {
        long key = resolveKeyAsLong(keyContext.currentKey);
        TimeWindow window = (TimeWindow) currentNamespace;
        byte[] bytes = NativeEngine.mapGetWithTW(stateHandle, key, keyContext.currentKeyGroupIndex,
                window.getStart(), window.getEnd(), serializeUserKey(userKey));
        return bytes == null ? null : deserializeUserValue(bytes);
    }

    private UV getOtherStrategies(UK userKey) {
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    long longUserKey = resolveUKAsLong(userKey);
                    int group = keyContext.currentKeyGroupIndex;
                    int cached = fixedMapEntry(group, k, longUserKey, 0, false);
                    if (cached == NamespaceReadIndex.NUMBER) return wrapLongAsUV(rootReadIndex.result);
                    if (cached == NamespaceReadIndex.ABSENT || !NativeEngine.mapGetLongLongSafe(stateHandle, k,
                            group, longUserKey, null))
                        return null;
                    return wrapLongAsUV(MemorySegmentBridge.readNativeLong(readResultAddress));
                case LONG_BYTES_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    long bytesUserKey = resolveUKAsLong(userKey);
                    int payloadGroup = keyContext.currentKeyGroupIndex;
                    int payload = rootReadIndex == null ? NamespaceReadIndex.UNKNOWN
                            : rootReadIndex.readMapPayload(payloadGroup, k, bytesUserKey);
                    if (payload == NamespaceReadIndex.ABSENT) return null;
                    if (payload == NamespaceReadIndex.ROW) {
                        if (listRowArity >= 0) {
                            List<?> owned = rootReadIndex.copyCurrentRowList(listRowArity);
                            if (owned != null) return (UV) owned;
                        }
                        return deserializeUserValue(rootReadIndex.copyCurrentRow());
                    }
                    byte[] lb = NativeEngine.mapGetLongBytes(stateHandle, k, payloadGroup, bytesUserKey);
                    return lb != null ? deserializeUserValue(lb) : null;
                case BYTES_LONG_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (userKey != null && absentRoot(k)) return null;
                    if (!NativeEngine.mapGetBytesLongSafe(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey), null))
                        return null;
                    return wrapLongAsUV(MemorySegmentBridge.readNativeLong(readResultAddress));
                }
                case LONG_MAP_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (userKey != null && absentRoot(k)) return null;
                    byte[] r = NativeEngine.mapGet(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                    return r != null ? deserializeUserValue(r) : null;
                }
                case LONG_MAP_TW:
                    return getWithTimeWindowNamespace(userKey);
                default: {
                    byte[] r = NativeEngine.mapGetGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                    return r != null ? deserializeUserValue(r) : null;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get from MapState", e);
        }
    }

    @Override
    public void put(UK userKey, UV userValue) {
        if (rowKeyIntMap) {
            putRowKeyInt(userKey, userValue);
        } else if (timeWindowMap) {
            putWindowMapping(userKey, userValue);
        } else if (rowKeyMap) {
            putRowKeyBytes(userKey, userValue);
        } else {
            putOtherStrategies(userKey, userValue);
        }
    }

    private void putRowKeyInt(UK userKey, UV userValue) {
        BinaryRowData row = contiguousUserKey(userKey);
        MemorySegment segment = row.getSegments()[0];
        long key = resolveKeyAsLong(keyContext.currentKey);
        int group = keyContext.currentKeyGroupIndex, size = row.getSizeInBytes(), offset = row.getOffset();
        if (rootReadIndex != null && rootReadIndex.updateRowMap(group, key, segment, offset, size, (Integer) userValue)) return;
        if (segment.isOffHeap()) NativeEngine.mapPutRowKeyIntAddress(stateHandle, key, group,
                row, segment.getAddress() + offset, size, (Integer) userValue);
        else NativeEngine.mapPutRowKeyInt(stateHandle, key, group, segment.getArray(), offset, size, (Integer) userValue);
    }

    private void putWindowMapping(UK userKey, UV userValue) {
        long key = resolveKeyAsLong(keyContext.currentKey);
        int group = keyContext.currentKeyGroupIndex;
        long start = TypeAnalyzer.windowStart(userKey), end = TypeAnalyzer.windowEnd(userKey);
        long mappedStart = TypeAnalyzer.windowStart(userValue), mappedEnd = TypeAnalyzer.windowEnd(userValue);
        if (rootReadIndex != null && rootReadIndex.updateFixedMap(group, key, start, end, mappedStart, mappedEnd, true)) return;
        NativeEngine.mapPutTimeWindow(stateHandle, key, group, start, end, mappedStart, mappedEnd);
    }

    private void putRowKeyBytes(UK userKey, UV userValue) {
        try {
            BinaryRowData row = contiguousUserKey(userKey);
            MemorySegment segment = row.getSegments()[0];
            long key = resolveKeyAsLong(keyContext.currentKey);
            int group = keyContext.currentKeyGroupIndex, size = row.getSizeInBytes(), offset = row.getOffset();
            byte[] value = serializeUserValue(userValue);
            if (segment.isOffHeap()) NativeEngine.mapPutRowKeyBytesAddress(stateHandle, key, group,
                    row, segment.getAddress() + offset, size, value);
            else NativeEngine.mapPutRowKeyBytes(stateHandle, key, group, segment.getArray(), offset, size, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to put to MapState", e);
        }
    }

    private void putOtherStrategies(UK userKey, UV userValue) {
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    int group = keyContext.currentKeyGroupIndex;
                    long inner = resolveUKAsLong(userKey), value = resolveUVAsLong(userValue);
                    if (rootReadIndex != null && rootReadIndex.updateFixedMap(group, k, inner, 0, value, 0, false)) return;
                    NativeEngine.mapPutLongLong(stateHandle, k, group, inner, value);
                    return;
                }
                case LONG_BYTES_VOID:
                    putLongBytes(resolveKeyAsLong(keyContext.currentKey), resolveUKAsLong(userKey), userValue);
                    return;
                case BYTES_LONG_VOID:
                    NativeEngine.mapPutBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey), resolveUVAsLong(userValue));
                    return;
                case LONG_MAP_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    NativeEngine.mapPut(stateHandle, k, keyContext.currentKeyGroupIndex,
                            serializeUserKey(userKey), serializeUserValue(userValue));
                    return;
                case LONG_MAP_TW: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.mapPutWithTW(stateHandle, k, keyContext.currentKeyGroupIndex,
                            tw.getStart(), tw.getEnd(), serializeUserKey(userKey), serializeUserValue(userValue));
                    return;
                }
                default:
                    NativeEngine.mapPutGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey), serializeUserValue(userValue));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to put to MapState", e);
        }
    }

    @Override
    public void putAll(Map<UK, UV> newMap) {
        if (newMap == null || newMap.isEmpty()) {
            return;
        }
        for (Map.Entry<UK, UV> entry : newMap.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Fused get-add-put fast path for Long-keyed, Long-valued MapState entries.
     *
     * <p>This is intentionally outside the Flink MapState interface and is
     * discovered only by ForL0-aware benchmarks/operators. It collapses the
     * common {@code get(userKey); put(userKey, old + delta)} pattern into one
     * JNI call and one native inner-map lookup.
     */
    public long addAndGetLong(Object userKey, long delta) {
        if (mapStrategy != MapStrategy.LONG_LONG_VOID) {
            throw new IllegalStateException("addAndGetLong requires Long key, Long user key, Long user value, and VoidNamespace");
        }
        return NativeEngine.mapAddAndGetLongLong(
                stateHandle,
                resolveKeyAsLong(keyContext.currentKey),
                keyContext.currentKeyGroupIndex,
                resolveUKAsLong((UK) userKey),
                delta);
    }

    /**
     * Batch variant for synthetic scalar probes that repeatedly touch adjacent
     * long user keys. Returns the sum of updated values.
     */
    public long addSequentialAndSumLong(long startUserKey, int count, long modulo, long delta) {
        if (mapStrategy != MapStrategy.LONG_LONG_VOID) {
            throw new IllegalStateException("addSequentialAndSumLong requires Long key, Long user key, Long user value, and VoidNamespace");
        }
        return NativeEngine.mapAddSequentialAndSumLongLong(
                stateHandle,
                resolveKeyAsLong(keyContext.currentKey),
                keyContext.currentKeyGroupIndex,
                startUserKey,
                count,
                modulo,
                delta);
    }

    /**
     * Batch read-only sum for adjacent long user keys. Missing user keys
     * contribute zero.
     */
    public long sumSequentialLong(long startUserKey, int count, long modulo) {
        if (mapStrategy != MapStrategy.LONG_LONG_VOID) {
            throw new IllegalStateException("sumSequentialLong requires Long key, Long user key, Long user value, and VoidNamespace");
        }
        return NativeEngine.mapSumSequentialLongLong(
                stateHandle,
                resolveKeyAsLong(keyContext.currentKey),
                keyContext.currentKeyGroupIndex,
                startUserKey,
                count,
                modulo);
    }

    @Override
    public void remove(UK userKey) {
        if (timeWindowMap) {
            removeWindowMapping(userKey);
        } else {
            removeOtherStrategies(userKey);
        }
    }

    private void removeWindowMapping(UK userKey) {
        NativeEngine.mapRemoveTimeWindow(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                keyContext.currentKeyGroupIndex, TypeAnalyzer.windowStart(userKey), TypeAnalyzer.windowEnd(userKey));
    }

    private void removeOtherStrategies(UK userKey) {
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                    NativeEngine.mapRemoveLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey));
                    return;
                case LONG_BYTES_VOID:
                    NativeEngine.mapRemoveLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey));
                    return;
                case BYTES_LONG_VOID:
                    NativeEngine.mapRemoveBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                    return;
                case LONG_MAP_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    NativeEngine.mapRemove(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                    return;
                case LONG_MAP_TW: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.mapRemoveWithTW(stateHandle, k, keyContext.currentKeyGroupIndex,
                            tw.getStart(), tw.getEnd(), serializeUserKey(userKey));
                    return;
                }
                default:
                    NativeEngine.mapRemoveGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove from MapState", e);
        }
    }

    @Override
    public boolean contains(UK userKey) {
        if (timeWindowMap) {
            long key = resolveKeyAsLong(keyContext.currentKey);
            int group = keyContext.currentKeyGroupIndex;
            long start = TypeAnalyzer.windowStart(userKey), end = TypeAnalyzer.windowEnd(userKey);
            int cached = fixedMapEntry(group, key, start, end, true);
            return cached == NamespaceReadIndex.UNKNOWN ? NativeEngine.mapGetTimeWindow(stateHandle,
                    key, group, start, end) : cached == NamespaceReadIndex.NUMBER;
        }
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    long innerKey = resolveUKAsLong(userKey);
                    int group = keyContext.currentKeyGroupIndex;
                    int cached = fixedMapEntry(group, k, innerKey, 0, false);
                    return cached == NamespaceReadIndex.UNKNOWN ? NativeEngine.mapContainsLongLong(stateHandle,
                            k, group, innerKey) : cached == NamespaceReadIndex.NUMBER;
                }
                case LONG_BYTES_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    long innerKey = resolveUKAsLong(userKey);
                    int group = keyContext.currentKeyGroupIndex;
                    int cached = rootReadIndex == null ? NamespaceReadIndex.UNKNOWN : rootReadIndex.readMapPayload(group, k, innerKey);
                    return cached == NamespaceReadIndex.UNKNOWN ? NativeEngine.mapContainsLongBytes(stateHandle, k, group, innerKey)
                            : cached == NamespaceReadIndex.ROW;
                }
                case BYTES_LONG_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (userKey != null && absentRoot(k)) return false;
                    return NativeEngine.mapContainsBytesLong(stateHandle, k,
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                case LONG_MAP_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (userKey != null && absentRoot(k)) return false;
                    return NativeEngine.mapContains(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                case LONG_MAP_TW: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    return NativeEngine.mapContainsWithTW(stateHandle, k, keyContext.currentKeyGroupIndex,
                            tw.getStart(), tw.getEnd(), serializeUserKey(userKey));
                }
                default:
                    return NativeEngine.mapContainsGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check contains in MapState", e);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        if (rootReadIndex != null && (rowKeyIntMap || mapStrategy == MapStrategy.LONG_BYTES_VOID)) {
            int match = rootReadIndex.readWholeMap(keyContext.currentKeyGroupIndex, resolveKeyAsLong(keyContext.currentKey));
            if (match == NamespaceReadIndex.ABSENT) return Collections.emptyList();
            if (match == NamespaceReadIndex.MAP) {
                byte[] copy = rootReadIndex.copyMapEntries(rowKeyIntMap);
                if (copy != null) return () -> new RemovableIterator(new BulkEntryIterator(copy));
            }
        } else if (absentCurrentRoot()) return Collections.emptyList();
        Iterable<Map.Entry<UK, UV>> snapshot = bulkEntries();
        return () -> new RemovableIterator(snapshot.iterator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<UK> keys() {
        if (rootReadIndex != null) {
            Iterable<UK> column = residentMapColumn(true);
            if (column != null) return column;
        }
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    if (arr == null) return Collections.emptySet();
                    List<UK> keys = new ArrayList<>(arr.length / 2);
                    for (int i = 0; i < arr.length; i += 2) keys.add(wrapLongAsUK(arr[i]));
                    return keys;
                }
                case LONG_BYTES_VOID: {
                    byte[] data = NativeEngine.mapEntriesLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesLongBytes(data).keySet() : Collections.emptySet();
                }
                case BYTES_LONG_VOID: {
                    byte[] data = NativeEngine.mapEntriesBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesBytesLong(data).keySet() : Collections.emptySet();
                }
                case LONG_MAP_VOID: {
                    byte[] data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntries(data).keySet() : Collections.emptySet();
                }
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] data = NativeEngine.mapEntriesWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return data != null ? deserializeEntries(data).keySet() : Collections.emptySet();
                }
                default: {
                    byte[] data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntries(data).keySet() : Collections.emptySet();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get keys from MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<UV> values() {
        if (rootReadIndex != null) {
            Iterable<UV> column = residentMapColumn(false);
            if (column != null) return column;
        }
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    if (arr == null) return Collections.emptyList();
                    List<UV> vals = new ArrayList<>(arr.length / 2);
                    for (int i = 1; i < arr.length; i += 2) vals.add(wrapLongAsUV(arr[i]));
                    return vals;
                }
                case LONG_BYTES_VOID: {
                    byte[] data = NativeEngine.mapEntriesLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesLongBytes(data).values() : Collections.emptyList();
                }
                case BYTES_LONG_VOID: {
                    byte[] data = NativeEngine.mapEntriesBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesBytesLong(data).values() : Collections.emptyList();
                }
                case LONG_MAP_VOID: {
                    byte[] data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntries(data).values() : Collections.emptyList();
                }
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] data = NativeEngine.mapEntriesWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return data != null ? deserializeEntries(data).values() : Collections.emptyList();
                }
                default: {
                    byte[] data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntries(data).values() : Collections.emptyList();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get values from MapState", e);
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        return entries().iterator();
    }

    @SuppressWarnings("unchecked")
    private <T> Iterable<T> residentMapColumn(boolean keys) {
        if (!(timeWindowMap || rowKeyIntMap || mapStrategy == MapStrategy.LONG_LONG_VOID
                || (keys && mapStrategy == MapStrategy.LONG_BYTES_VOID))) {
            return absentCurrentRoot() ? Collections.emptyList() : null;
        }
        int match = rootReadIndex.readWholeMap(keyContext.currentKeyGroupIndex, resolveKeyAsLong(keyContext.currentKey));
        if (match == NamespaceReadIndex.ABSENT) return Collections.emptyList();
        if (match != NamespaceReadIndex.MAP) return null;
        if (keys && rowKeyIntMap) return (Iterable<T>) rootReadIndex.copyMapRowKeys(rowDataUKAccessor.getArity());
        long[] column = rootReadIndex.copyMapNumbers(keys);
        if (column == null) return null;
        if (timeWindowMap) {
            List<T> windows = new ArrayList<>(column.length / 2);
            TypeSerializer<?> serializer = keys ? userKeySerializer : userValueSerializer;
            for (int i = 0; i < column.length; i += 2)
                windows.add((T) TypeAnalyzer.timeWindow(serializer, column[i], column[i + 1]));
            return windows;
        }
        List<T> numbers = new ArrayList<>(column.length);
        for (long number : column) {
            if (rowKeyIntMap) numbers.add((T) Integer.valueOf((int) number));
            else numbers.add((T) (keys ? wrapLongAsUK(number) : wrapLongAsUV(number)));
        }
        return numbers;
    }

    // The native bulk format is already serializer-compatible. Iterate one
    // owned byte array lazily, wrapping RowData directly without per-row copies.
    private Iterable<Map.Entry<UK, UV>> bulkEntries() {
        try {
            final byte[] data;
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    final long[] rows = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    return () -> new Iterator<Map.Entry<UK, UV>>() {
                        private int position;
                        public boolean hasNext() { return rows != null && position < rows.length; }
                        public Map.Entry<UK, UV> next() {
                            if (!hasNext()) throw new NoSuchElementException();
                            Map.Entry<UK, UV> entry = new AbstractMap.SimpleImmutableEntry<>(wrapLongAsUK(rows[position]), wrapLongAsUV(rows[position + 1]));
                            position += 2;
                            return entry;
                        }
                    };
                }
                case LONG_BYTES_VOID:
                    data = NativeEngine.mapEntriesLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    break;
                case BYTES_LONG_VOID:
                    data = NativeEngine.mapEntriesBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    break;
                case LONG_MAP_VOID:
                    data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    break;
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    data = NativeEngine.mapEntriesWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    break;
                }
                default:
                    data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey), keyContext.currentKeyGroupIndex);
            }
            if (data == null) return Collections.emptySet();
            return () -> new BulkEntryIterator(data);
        } catch (IOException e) { throw new RuntimeException("Failed to read MapState entries", e); }
    }

    private class BulkEntryIterator implements Iterator<Map.Entry<UK, UV>> {
        private final byte[] data;
        private final DataInputDeserializer input;
        private final org.apache.flink.core.memory.MemorySegment segment;
        private int remaining;
        BulkEntryIterator(byte[] data) {
            this.data = data;
            this.input = new DataInputDeserializer(data);
            this.segment = org.apache.flink.core.memory.MemorySegmentFactory.wrap(data);
            try { remaining = input.readInt(); }
            catch (IOException e) { throw new RuntimeException(e); }
        }
        public boolean hasNext() { return remaining > 0; }
        @SuppressWarnings("unchecked")
        private <T> T read(TypeSerializer<T> serializer, boolean row, RowDataKeyAccessor accessor) throws IOException {
            if (!row) return serializer.deserialize(input);
            int size = input.readInt();
            BinaryRowData value = new BinaryRowData(accessor.getArity());
            value.pointTo(segment, data.length - input.available(), size);
            input.skipBytesToRead(size);
            return (T) value;
        }
        public Map.Entry<UK, UV> next() {
            if (!hasNext()) throw new NoSuchElementException();
            try {
                UK key;
                UV value;
                if (mapStrategy == MapStrategy.LONG_BYTES_VOID) {
                    key = wrapLongAsUK(input.readLong());
                    input.readInt(); // outer UV byte length; nested serializer is self-delimiting
                    value = read(userValueSerializer, isRowDataUV, rowDataUVAccessor);
                } else if (mapStrategy == MapStrategy.BYTES_LONG_VOID) {
                    input.readInt(); // outer UK byte length
                    key = read(userKeySerializer, isRowDataUK, rowDataUKAccessor);
                    value = wrapLongAsUV(input.readLong());
                } else {
                    key = read(userKeySerializer, isRowDataUK, rowDataUKAccessor);
                    value = read(userValueSerializer, isRowDataUV, rowDataUVAccessor);
                }
                remaining--;
                return new AbstractMap.SimpleImmutableEntry<>(key, value);
            } catch (IOException e) { throw new RuntimeException("Failed to read MapState entry", e); }
        }
    }

    /**
     * Iterator wrapper that implements {@link Iterator#remove()} by calling
     * {@link ForL0MapState#remove(Object)} on the last-returned user key.
     * Required for Flink's MapState contract — user code (e.g. event-time
     * cleanup) commonly calls {@code iterator.remove()} to delete entries.
     */
    private class RemovableIterator implements Iterator<Map.Entry<UK, UV>> {
        private final Iterator<Map.Entry<UK, UV>> delegate;
        private UK lastKey;
        private boolean canRemove;

        RemovableIterator(Iterator<Map.Entry<UK, UV>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Map.Entry<UK, UV> next() {
            Map.Entry<UK, UV> entry = delegate.next();
            lastKey = entry.getKey();
            canRemove = true;
            return entry;
        }

        @Override
        public void remove() {
            if (!canRemove) {
                throw new IllegalStateException("next() has not been called, or remove() has already been called after the last call to next()");
            }
            ForL0MapState.this.remove(lastKey);
            canRemove = false;
        }
    }

    @Override
    public boolean isEmpty() {
        if (rootReadIndex != null) {
            int match = rootReadIndex.readWholeMap(keyContext.currentKeyGroupIndex, resolveKeyAsLong(keyContext.currentKey));
            if (match == NamespaceReadIndex.ABSENT) return true;
            if (match == NamespaceReadIndex.MAP) return rootReadIndex.currentMapIsEmpty();
        }
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                case LONG_BYTES_VOID:
                case BYTES_LONG_VOID:
                case LONG_MAP_VOID:
                    return NativeEngine.mapIsEmpty(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] data = NativeEngine.mapEntriesWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return data == null;
                }
                default:
                    return NativeEngine.mapIsEmptyGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check isEmpty on MapState", e);
        }
    }

    @Override
    public void clear() {
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                case LONG_BYTES_VOID:
                case BYTES_LONG_VOID:
                case LONG_MAP_VOID:
                    NativeEngine.mapClear(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return;
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.mapClearWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return;
                }
                default:
                    NativeEngine.mapClearGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear MapState", e);
        }
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException("State incremental visitor not supported by ForL0StateBackend");
    }

    // ========== Serialization helpers ==========

    private int fixedMapEntry(int group, long key, long first, long second, boolean windows) {
        return rootReadIndex == null ? NamespaceReadIndex.UNKNOWN
                : rootReadIndex.readMapEntry(group, key, first, second, windows);
    }

    private boolean absentRoot(long key) {
        return rootReadIndex != null && rootReadIndex.absentNumber(keyContext.currentKeyGroupIndex, key);
    }

    private boolean absentCurrentRoot() {
        return rootReadIndex != null && rootReadIndex.absentNumber(
                keyContext.currentKeyGroupIndex, resolveKeyAsLong(keyContext.currentKey));
    }

    /** Resolve key to long — works for both primitive Long and RowData[BIGINT]. */
    private long resolveKeyAsLong(K key) {
        if (isRowDataKey) {
            return rowDataKeyAccessor.extractSingleLong(key);
        }
        return (Long) key;
    }

    @SuppressWarnings("unchecked")
    private byte[] serializeKey(K key) throws IOException {
        keyOut.clear();
        if (!voidNamespace) {
            ((TypeSerializer<N>) namespaceSerializer).serialize(currentNamespace, keyOut);
        }
        keySerializer.serialize(key, keyOut);
        return keyOut.getCopyOfBuffer();
    }

    private byte[] serializeUserKey(UK userKey) throws IOException {
        if (isRowDataUK) {
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(binaryUserKey(userKey));
            if (raw != null) return raw;
        }
        ukOut.clear();
        userKeySerializer.serialize(userKey, ukOut);
        return ukOut.getCopyOfBuffer();
    }

    private byte[] serializeUserValue(UV userValue) throws IOException {
        if (isRowDataUV) {
            BinaryRowData row = TypeAnalyzer.toBinaryRow(
                    userValueSerializer, (org.apache.flink.table.data.RowData) userValue);
            if (row.getSegments().length != 1 || row.getOffset() > row.getSegments()[0].size() - row.getSizeInBytes()) row = row.copy();
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(row);
            if (raw != null) return raw;
        }
        uvOut.clear();
        userValueSerializer.serialize(userValue, uvOut);
        return uvOut.getCopyOfBuffer();
    }

    private void putLongBytes(long key, long innerKey, UV value) throws IOException {
        TypeSerializer<?> serializer = null;
        Object element = value;
        if (isRowDataUV) {
            serializer = userValueSerializer;
        } else if (listRowElementSerializer != null && value instanceof List && ((List<?>) value).size() == 1) {
            element = ((List<?>) value).get(0);
            if (element instanceof org.apache.flink.table.data.RowData) serializer = listRowElementSerializer;
        }
        int group = keyContext.currentKeyGroupIndex;
        if (serializer == null) {
            uvOut.clear();
            userValueSerializer.serialize(value, uvOut);
            if (rootReadIndex != null && rootReadIndex.updateMapBytes(group, key, innerKey, uvOut.getSharedBuffer(), uvOut.length())) return;
            NativeEngine.mapPutLongBytesRange(stateHandle, key, group, innerKey, uvOut.getSharedBuffer(), 0, uvOut.length());
            return;
        }
        BinaryRowData row = TypeAnalyzer.toBinaryRow(serializer, (org.apache.flink.table.data.RowData) element);
        MemorySegment segment = row.getSegments()[0];
        // An off-heap input can be borrowed from this very table. Own it before mutation.
        if (row.getSegments().length != 1 || segment.isOffHeap() || row.getOffset() > segment.size() - row.getSizeInBytes()) {
            row = row.copy();
            segment = row.getSegments()[0];
        }
        boolean list = !isRowDataUV;
        if (rootReadIndex != null && rootReadIndex.updateMapRow(group, key, innerKey, segment, row.getOffset(), row.getSizeInBytes(), list)) return;
        NativeEngine.mapPutLongRowRange(stateHandle, key, group, innerKey, segment.getArray(), row.getOffset(), row.getSizeInBytes(), list);
    }

    private BinaryRowData contiguousUserKey(UK key) {
        BinaryRowData row = TypeAnalyzer.toBinaryRow(userKeySerializer, (org.apache.flink.table.data.RowData) key);
        MemorySegment[] segments = row.getSegments();
        if (segments.length == 1 && row.getOffset() >= 0 && row.getOffset() <= segments[0].size() - row.getSizeInBytes()) return row;
        return row.copy();
    }

    private BinaryRowData binaryUserKey(UK key) {
        BinaryRowData row = contiguousUserKey(key);
        return row.getSegments()[0].isOffHeap() ? row.copy() : row;
    }

    @SuppressWarnings("unchecked")
    private UV deserializeUserValue(byte[] bytes) throws IOException {
        if (isRowDataUV) {
            return (UV) RowDataKeyAccessor.wrapBinaryRowDataCompat(bytes, rowDataUVAccessor.getArity());
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        return userValueSerializer.deserialize(in);
    }

    /** Deserialize entries from C++: [count (int)] + [uk_bytes, uv_bytes ...]. */
    @SuppressWarnings("unchecked")
    private Map<UK, UV> deserializeEntries(byte[] data) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(data);
        int count = in.readInt();
        Map<UK, UV> map = new HashMap<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) {
            UK uk;
            if (isRowDataUK) {
                // Zero-copy: read [4-byte size][raw] produced by extractBinaryRowDataBytesCompat
                int ukSize = in.readInt();
                byte[] ukRaw = new byte[ukSize];
                in.readFully(ukRaw);
                uk = (UK) RowDataKeyAccessor.wrapBinaryRowData(ukRaw, rowDataUKAccessor.getArity());
            } else {
                uk = userKeySerializer.deserialize(in);
            }
            UV uv;
            if (isRowDataUV) {
                int uvSize = in.readInt();
                byte[] uvRaw = new byte[uvSize];
                in.readFully(uvRaw);
                uv = (UV) RowDataKeyAccessor.wrapBinaryRowData(uvRaw, rowDataUVAccessor.getArity());
            } else {
                uv = userValueSerializer.deserialize(in);
            }
            map.put(uk, uv);
        }
        return map;
    }

    /** Deserialize LongBytes entries: [count(4B)][uk0(8B)][uv0_len(4B)][uv0_bytes]... */
    @SuppressWarnings("unchecked")
    private Map<UK, UV> deserializeEntriesLongBytes(byte[] data) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(data);
        int count = in.readInt();
        Map<UK, UV> map = new HashMap<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) {
            long uk = in.readLong();
            int uvLen = in.readInt();
            byte[] uvBytes = new byte[uvLen];
            in.readFully(uvBytes);
            map.put(wrapLongAsUK(uk), deserializeUserValue(uvBytes));
        }
        return map;
    }

    /** Deserialize BytesLong entries: [count(4B)][uk0_len(4B)][uk0_bytes][uv0(8B)]... */
    @SuppressWarnings("unchecked")
    private Map<UK, UV> deserializeEntriesBytesLong(byte[] data) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(data);
        int count = in.readInt();
        Map<UK, UV> map = new HashMap<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) {
            int ukLen = in.readInt();
            byte[] ukBytes = new byte[ukLen];
            in.readFully(ukBytes);
            UK uk;
            if (isRowDataUK) {
                // Zero-copy: ukBytes is [4-byte size][raw BinaryRowData]
                uk = (UK) RowDataKeyAccessor.wrapBinaryRowDataCompat(ukBytes, rowDataUKAccessor.getArity());
            } else {
                DataInputDeserializer ukIn = new DataInputDeserializer(ukBytes);
                uk = userKeySerializer.deserialize(ukIn);
            }
            long uv = in.readLong();
            map.put(uk, wrapLongAsUV(uv));
        }
        return map;
    }

    // ========== Factory Methods ==========

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            long stateHandle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            ForL0KeyedStateBackend<K> backend) {
        TypeSerializer userKeySerializer;
        TypeSerializer userValueSerializer;
        if (stateSerializer instanceof MapSerializer) {
            MapSerializer mapSer = (MapSerializer) stateSerializer;
            userKeySerializer = mapSer.getKeySerializer();
            userValueSerializer = mapSer.getValueSerializer();
        } else {
            userKeySerializer = null;
            userValueSerializer = null;
        }
        return (IS) new ForL0MapState<>(
                stateHandle,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                namespaceSerializer,
                (TypeSerializer) stateSerializer,
                userKeySerializer,
                userValueSerializer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            IS existingState) {
        TypeSerializer userKeySerializer;
        TypeSerializer userValueSerializer;
        if (stateSerializer instanceof MapSerializer) {
            MapSerializer mapSer = (MapSerializer) stateSerializer;
            userKeySerializer = mapSer.getKeySerializer();
            userValueSerializer = mapSer.getValueSerializer();
        } else {
            userKeySerializer = null;
            userValueSerializer = null;
        }
        return (IS) ((ForL0MapState) existingState)
                .setNamespaceSerializer(namespaceSerializer)
                .setValueSerializer((TypeSerializer) stateSerializer)
                .setUserKeySerializer(userKeySerializer)
                .setUserValueSerializer(userValueSerializer);
    }

    ForL0MapState<K, N, UK, UV> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0MapState<K, N, UK, UV> setValueSerializer(TypeSerializer<Map<UK, UV>> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0MapState<K, N, UK, UV> setUserKeySerializer(TypeSerializer<UK> userKeySerializer) {
        this.userKeySerializer = userKeySerializer;
        return this;
    }

    ForL0MapState<K, N, UK, UV> setUserValueSerializer(TypeSerializer<UV> userValueSerializer) {
        configureUserValueSerializer(userValueSerializer);
        return this;
    }

    private void configureUserValueSerializer(TypeSerializer<UV> userValueSerializer) {
        this.userValueSerializer = userValueSerializer;
        this.listRowElementSerializer = TypeAnalyzer.standardListRowElement(userValueSerializer);
        LogicalType[] fields = TypeAnalyzer.getRowDataFieldTypes(listRowElementSerializer);
        listRowArity = fields == null ? -1 : fields.length;
    }
}
