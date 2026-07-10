package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
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
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<Map<UK, UV>> valueSerializer;
    private TypeSerializer<UK> userKeySerializer;
    private TypeSerializer<UV> userValueSerializer;
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
    private final long[] primitiveBuf = new long[1];

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
        this.userValueSerializer = userValueSerializer;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        // Use TypeAnalyzer (same as C++ registration) to detect INT64 — handles both
        // raw LongSerializer and RowData-wrapped BIGINT.
        this.ukTypeId = effectiveInt64TypeId(userKeySerializer);
        this.uvTypeId = effectiveInt64TypeId(userValueSerializer);
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataUK = userKeySerializer != null && TypeAnalyzer.isRowDataSerializer(userKeySerializer);
        this.rowDataUKAccessor = isRowDataUK ? TypeAnalyzer.createRowDataKeyAccessor(userKeySerializer) : null;
        this.isRowDataUV = userValueSerializer != null && TypeAnalyzer.isRowDataSerializer(userValueSerializer);
        this.rowDataUVAccessor = isRowDataUV ? TypeAnalyzer.createRowDataKeyAccessor(userValueSerializer) : null;

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
            return (UK) rowDataUKAccessor.reconstructFromLong(value);
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
    @Override
    public UV get(UK userKey) {
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (!NativeEngine.mapGetLongLongSafe(stateHandle, k, keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey), primitiveBuf))
                        return null;
                    return wrapLongAsUV(primitiveBuf[0]);
                case LONG_BYTES_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
                    byte[] lb = NativeEngine.mapGetLongBytes(stateHandle, k, keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey));
                    return lb != null ? deserializeUserValue(lb) : null;
                case BYTES_LONG_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    if (!NativeEngine.mapGetBytesLongSafe(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey), primitiveBuf))
                        return null;
                    return wrapLongAsUV(primitiveBuf[0]);
                }
                case LONG_MAP_VOID: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    byte[] r = NativeEngine.mapGet(stateHandle, k, keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                    return r != null ? deserializeUserValue(r) : null;
                }
                case LONG_MAP_TW: {
                    k = resolveKeyAsLong(keyContext.currentKey);
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] r = NativeEngine.mapGetWithTW(stateHandle, k, keyContext.currentKeyGroupIndex,
                            tw.getStart(), tw.getEnd(), serializeUserKey(userKey));
                    return r != null ? deserializeUserValue(r) : null;
                }
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
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                    NativeEngine.mapPutLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey), resolveUVAsLong(userValue));
                    return;
                case LONG_BYTES_VOID:
                    NativeEngine.mapPutLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey), serializeUserValue(userValue));
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
        try {
            long k;
            switch (mapStrategy) {
                case LONG_LONG_VOID:
                    return NativeEngine.mapContainsLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey));
                case LONG_BYTES_VOID:
                    return NativeEngine.mapContainsLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, resolveUKAsLong(userKey));
                case BYTES_LONG_VOID:
                    return NativeEngine.mapContainsBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeUserKey(userKey));
                case LONG_MAP_VOID:
                    k = resolveKeyAsLong(keyContext.currentKey);
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

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    if (arr == null) return Collections.emptySet();
                    Map<UK, UV> map = new HashMap<>(arr.length / 2 * 4 / 3 + 1);
                    for (int i = 0; i < arr.length; i += 2)
                        map.put(wrapLongAsUK(arr[i]), wrapLongAsUV(arr[i + 1]));
                    return map.entrySet();
                }
                case LONG_BYTES_VOID: {
                    byte[] data = NativeEngine.mapEntriesLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesLongBytes(data).entrySet() : Collections.emptySet();
                }
                case BYTES_LONG_VOID: {
                    byte[] data = NativeEngine.mapEntriesBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? deserializeEntriesBytesLong(data).entrySet() : Collections.emptySet();
                }
                case LONG_MAP_VOID: {
                    // OPT-8: Lazy iteration — don't materialize full HashMap
                    final long k = resolveKeyAsLong(keyContext.currentKey);
                    final int kg = keyContext.currentKeyGroupIndex;
                    return () -> {
                        long iterH = NativeEngine.mapIteratorCreate(stateHandle, k, kg);
                        return iterH != 0 ? new NativeMapEntryIterator(iterH) : Collections.emptyIterator();
                    };
                }
                case LONG_MAP_TW: {
                    // OPT-8: Lazy iteration with TimeWindow namespace
                    final long k = resolveKeyAsLong(keyContext.currentKey);
                    final int kg = keyContext.currentKeyGroupIndex;
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    final long nsStart = tw.getStart(), nsEnd = tw.getEnd();
                    return () -> {
                        long iterH = NativeEngine.mapIteratorCreateWithTW(stateHandle, k, kg, nsStart, nsEnd);
                        return iterH != 0 ? new NativeMapEntryIterator(iterH) : Collections.emptyIterator();
                    };
                }
                default: {
                    // OPT-8: Lazy iteration for generic strategy
                    final byte[] keyBytes;
                    try {
                        keyBytes = serializeKey(keyContext.currentKey);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to serialize key", e);
                    }
                    final int kg = keyContext.currentKeyGroupIndex;
                    return () -> {
                        long iterH = NativeEngine.mapIteratorCreateGeneric(stateHandle, keyBytes, kg);
                        return iterH != 0 ? new NativeMapEntryIterator(iterH) : Collections.emptyIterator();
                    };
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get entries from MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<UK> keys() {
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

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        try {
            switch (mapStrategy) {
                case LONG_LONG_VOID: {
                    long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    if (arr == null) return Collections.emptyIterator();
                    Map<UK, UV> map = new HashMap<>(arr.length / 2 * 4 / 3 + 1);
                    for (int i = 0; i < arr.length; i += 2)
                        map.put(wrapLongAsUK(arr[i]), wrapLongAsUV(arr[i + 1]));
                    return new RemovableIterator(map.entrySet().iterator());
                }
                case LONG_BYTES_VOID: {
                    byte[] data = NativeEngine.mapEntriesLongBytes(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? new RemovableIterator(deserializeEntriesLongBytes(data).entrySet().iterator()) : Collections.emptyIterator();
                }
                case BYTES_LONG_VOID: {
                    byte[] data = NativeEngine.mapEntriesBytesLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex);
                    return data != null ? new RemovableIterator(deserializeEntriesBytesLong(data).entrySet().iterator()) : Collections.emptyIterator();
                }
                case LONG_MAP_VOID: {
                    long iterH = NativeEngine.mapIteratorCreate(stateHandle,
                            resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    return iterH != 0 ? new RemovableIterator(new NativeMapEntryIterator(iterH)) : Collections.emptyIterator();
                }
                case LONG_MAP_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    long iterH = NativeEngine.mapIteratorCreateWithTW(stateHandle,
                            resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex,
                            tw.getStart(), tw.getEnd());
                    return iterH != 0 ? new RemovableIterator(new NativeMapEntryIterator(iterH)) : Collections.emptyIterator();
                }
                default: {
                    long iterH = NativeEngine.mapIteratorCreateGeneric(stateHandle,
                            serializeKey(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    return iterH != 0 ? new RemovableIterator(new NativeMapEntryIterator(iterH)) : Collections.emptyIterator();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate MapState", e);
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
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(userKey);
            if (raw != null) return raw;
        }
        ukOut.clear();
        userKeySerializer.serialize(userKey, ukOut);
        return ukOut.getCopyOfBuffer();
    }

    private byte[] serializeUserValue(UV userValue) throws IOException {
        if (isRowDataUV) {
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(userValue);
            if (raw != null) return raw;
        }
        uvOut.clear();
        userValueSerializer.serialize(userValue, uvOut);
        return uvOut.getCopyOfBuffer();
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

    // ========== Streaming iterator for generic InnerMap ==========

    /**
     * Lazy iterator backed by a native C++ InnerMap iterator.
     * Each {@link #next()} call fetches one entry via JNI, avoiding full
     * materialization of the map in Java. Auto-destroys when exhausted.
     */
    private class NativeMapEntryIterator implements Iterator<Map.Entry<UK, UV>> {
        private long iterHandle;
        private byte[] prefetched;

        NativeMapEntryIterator(long iterHandle) {
            this.iterHandle = iterHandle;
            this.prefetched = NativeEngine.mapIteratorNext(iterHandle);
        }

        @Override
        public boolean hasNext() {
            return prefetched != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map.Entry<UK, UV> next() {
            if (prefetched == null) throw new NoSuchElementException();
            byte[] current = prefetched;
            prefetched = NativeEngine.mapIteratorNext(iterHandle);
            if (prefetched == null) {
                NativeEngine.mapIteratorDestroy(iterHandle);
                iterHandle = 0;
            }
            try {
                DataInputDeserializer in = new DataInputDeserializer(current);
                UK uk;
                if (isRowDataUK) {
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
                return new AbstractMap.SimpleImmutableEntry<>(uk, uv);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize MapState entry", e);
            }
        }

        @Override
        @SuppressWarnings("deprecation")
        protected void finalize() {
            if (iterHandle != 0) {
                NativeEngine.mapIteratorDestroy(iterHandle);
            }
        }
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
        this.userValueSerializer = userValueSerializer;
        return this;
    }
}
