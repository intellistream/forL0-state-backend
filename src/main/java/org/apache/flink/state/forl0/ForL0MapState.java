package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    private final boolean isRowDataKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataUK;
    private final RowDataKeyAccessor rowDataUKAccessor;
    private final boolean isRowDataUV;
    private final RowDataKeyAccessor rowDataUVAccessor;

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
        this.userValueSerializer = userValueSerializer;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        this.ukTypeId = isPrimitiveLong(userKeySerializer) ? TypeAnalyzer.TYPE_INT64 : -1;
        this.uvTypeId = isPrimitiveLong(userValueSerializer) ? TypeAnalyzer.TYPE_INT64 : -1;
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataUK = userKeySerializer != null && TypeAnalyzer.isRowDataSerializer(userKeySerializer);
        this.rowDataUKAccessor = isRowDataUK ? TypeAnalyzer.createRowDataKeyAccessor(userKeySerializer) : null;
        this.isRowDataUV = userValueSerializer != null && TypeAnalyzer.isRowDataSerializer(userValueSerializer);
        this.rowDataUVAccessor = isRowDataUV ? TypeAnalyzer.createRowDataKeyAccessor(userValueSerializer) : null;
    }

    /** True only for actual LongSerializer — excludes RowData-wrapped BIGINT. */
    private static boolean isPrimitiveLong(TypeSerializer<?> s) {
        return s instanceof org.apache.flink.api.common.typeutils.base.LongSerializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public UV get(UK userKey) {
        try {
            if (useLongLongMapPath()) {
                long uk = (Long) userKey;
                if (!NativeEngine.mapContainsLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, uk)) {
                    return null;
                }
                long result = NativeEngine.mapGetLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, uk);
                return (UV) Long.valueOf(result);
            }
            byte[] ukBytes = serializeUserKey(userKey);
            byte[] result;
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                result = NativeEngine.mapGet(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            } else {
                result = NativeEngine.mapGetGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            }
            return result != null ? deserializeUserValue(result) : null;
        } catch (IOException e) {
            throw new RuntimeException("Failed to get from MapState", e);
        }
    }

    @Override
    public void put(UK userKey, UV userValue) {
        try {
            if (useLongLongMapPath()) {
                NativeEngine.mapPutLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, (Long) userKey, (Long) userValue);
                return;
            }
            byte[] ukBytes = serializeUserKey(userKey);
            byte[] uvBytes = serializeUserValue(userValue);
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.mapPut(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes, uvBytes);
            } else {
                NativeEngine.mapPutGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes, uvBytes);
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

    @Override
    public void remove(UK userKey) {
        try {
            if (useLongLongMapPath()) {
                NativeEngine.mapRemoveLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, (Long) userKey);
                return;
            }
            byte[] ukBytes = serializeUserKey(userKey);
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.mapRemove(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            } else {
                NativeEngine.mapRemoveGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove from MapState", e);
        }
    }

    @Override
    public boolean contains(UK userKey) {
        try {
            if (useLongLongMapPath()) {
                return NativeEngine.mapContainsLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, (Long) userKey);
            }
            byte[] ukBytes = serializeUserKey(userKey);
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                return NativeEngine.mapContains(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            } else {
                return NativeEngine.mapContainsGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex, ukBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check contains in MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        try {
            if (useLongLongMapPath()) {
                long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
                if (arr == null) return Collections.emptySet();
                Map<UK, UV> map = new HashMap<>(arr.length / 2 * 4 / 3 + 1);
                for (int i = 0; i < arr.length; i += 2) {
                    map.put((UK) Long.valueOf(arr[i]), (UV) Long.valueOf(arr[i + 1]));
                }
                return map.entrySet();
            }
            byte[] data;
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
                data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            }
            if (data == null) return Collections.emptySet();
            return deserializeEntries(data).entrySet();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get entries from MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<UK> keys() {
        try {
            if (useLongLongMapPath()) {
                long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
                if (arr == null) return Collections.emptySet();
                List<UK> keys = new ArrayList<>(arr.length / 2);
                for (int i = 0; i < arr.length; i += 2) {
                    keys.add((UK) Long.valueOf(arr[i]));
                }
                return keys;
            }
            byte[] data;
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
                data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            }
            if (data == null) return Collections.emptySet();
            return deserializeEntries(data).keySet();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get keys from MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<UV> values() {
        try {
            if (useLongLongMapPath()) {
                long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
                if (arr == null) return Collections.emptyList();
                List<UV> vals = new ArrayList<>(arr.length / 2);
                for (int i = 1; i < arr.length; i += 2) {
                    vals.add((UV) Long.valueOf(arr[i]));
                }
                return vals;
            }
            byte[] data;
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
                data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            }
            if (data == null) return Collections.emptyList();
            return deserializeEntries(data).values();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get values from MapState", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        try {
            if (useLongLongMapPath()) {
                long[] arr = NativeEngine.mapEntriesLongLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
                if (arr == null) return Collections.emptyIterator();
                Map<UK, UV> map = new HashMap<>(arr.length / 2 * 4 / 3 + 1);
                for (int i = 0; i < arr.length; i += 2) {
                    map.put((UK) Long.valueOf(arr[i]), (UV) Long.valueOf(arr[i + 1]));
                }
                return map.entrySet().iterator();
            }
            byte[] data;
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                data = NativeEngine.mapEntries(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
                data = NativeEngine.mapEntriesGeneric(stateHandle, serializeKey(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            }
            if (data == null) return Collections.emptyIterator();
            return deserializeEntries(data).entrySet().iterator();
        } catch (IOException e) {
            throw new RuntimeException("Failed to iterate MapState", e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                return NativeEngine.mapIsEmpty(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
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
            if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
                NativeEngine.mapClear(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                        keyContext.currentKeyGroupIndex);
            } else {
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

    /** Whether Long key + Long UK + Long UV zero-serialization path is available. */
    private boolean useLongLongMapPath() {
        return voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64
                && ukTypeId == TypeAnalyzer.TYPE_INT64 && uvTypeId == TypeAnalyzer.TYPE_INT64;
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
    private Map<UK, UV> deserializeEntries(byte[] data) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(data);
        int count = in.readInt();
        Map<UK, UV> map = new HashMap<>(count * 4 / 3 + 1);
        for (int i = 0; i < count; i++) {
            UK uk = userKeySerializer.deserialize(in);
            UV uv = userValueSerializer.deserialize(in);
            map.put(uk, uv);
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
        this.userValueSerializer = userValueSerializer;
        return this;
    }
}
