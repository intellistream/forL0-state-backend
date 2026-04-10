package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * ForL0 implementation of {@link InternalListState}.
 *
 * <p>JNI thin shell — all state storage is in the C++ engine.
 *
 * @param <K> The type of key
 * @param <N> The type of namespace
 * @param <V> The type of list elements
 */
public class ForL0ListState<K, N, V> implements InternalListState<K, N, V> {

    private final long stateHandle;
    private final ForL0KeyContext<K> keyContext;
    private final TypeSerializer<K> keySerializer;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> elementSerializer;
    private TypeSerializer<List<V>> valueSerializer;
    private List<V> defaultValue;
    private N currentNamespace;

    private final int keyTypeId;
    private final int elementTypeId;
    private final boolean voidNamespace;
    private final boolean isTimeWindowNs;
    private final boolean isRowDataKey;
    private final RowDataKeyAccessor rowDataKeyAccessor;
    private final boolean isRowDataElement;
    private final RowDataKeyAccessor rowDataElementAccessor;

    private enum ListStrategy { LONG_ELEM_LONG_VOID, LONG_VOID, LONG_TW, GENERIC }
    private final ListStrategy listStrategy;

    /** Reusable serialization buffers (single-threaded access). */
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer elemOut = new DataOutputSerializer(128);

    ForL0ListState(
            long stateHandle,
            ForL0KeyContext<K> keyContext,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> elementSerializer,
            TypeSerializer<List<V>> valueSerializer,
            List<V> defaultValue) {
        this.stateHandle = stateHandle;
        this.keyContext = keyContext;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.elementSerializer = elementSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
        this.keyTypeId = TypeAnalyzer.getTypeId(keySerializer);
        this.elementTypeId = (elementSerializer instanceof org.apache.flink.api.common.typeutils.base.LongSerializer)
                ? TypeAnalyzer.TYPE_INT64 : -1;
        this.voidNamespace = namespaceSerializer instanceof VoidNamespaceSerializer;
        this.isTimeWindowNs = TypeAnalyzer.isTimeWindowSerializer(namespaceSerializer);
        this.isRowDataKey = TypeAnalyzer.isRowDataSerializer(keySerializer);
        this.rowDataKeyAccessor = isRowDataKey ? TypeAnalyzer.createRowDataKeyAccessor(keySerializer) : null;
        this.isRowDataElement = elementSerializer != null && TypeAnalyzer.isRowDataSerializer(elementSerializer);
        this.rowDataElementAccessor = isRowDataElement
                ? TypeAnalyzer.createRowDataKeyAccessor(elementSerializer) : null;

        if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64 && elementTypeId == TypeAnalyzer.TYPE_INT64) {
            this.listStrategy = ListStrategy.LONG_ELEM_LONG_VOID;
        } else if (voidNamespace && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.listStrategy = ListStrategy.LONG_VOID;
        } else if (isTimeWindowNs && keyTypeId == TypeAnalyzer.TYPE_INT64) {
            this.listStrategy = ListStrategy.LONG_TW;
        } else {
            this.listStrategy = ListStrategy.GENERIC;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<V> get() {
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID: {
                    long[] arr = NativeEngine.listGetLongElements(stateHandle,
                            resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (arr == null) return defaultValue != null ? valueSerializer.copy(defaultValue) : Collections.emptyList();
                    List<V> list = new ArrayList<>(arr.length);
                    for (long v : arr) { list.add((V) Long.valueOf(v)); }
                    return list;
                }
                case LONG_VOID: {
                    byte[] data = NativeEngine.listGet(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (data == null) return defaultValue != null ? valueSerializer.copy(defaultValue) : Collections.emptyList();
                    return deserializeList(data);
                }
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] data = NativeEngine.listGetWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    if (data == null) return defaultValue != null ? valueSerializer.copy(defaultValue) : Collections.emptyList();
                    return deserializeList(data);
                }
                default: {
                    byte[] data = NativeEngine.listGetGeneric(stateHandle, serializeKey(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (data == null) return defaultValue != null ? valueSerializer.copy(defaultValue) : Collections.emptyList();
                    return deserializeList(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get ListState", e);
        }
    }

    @Override
    public void add(V value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID:
                    NativeEngine.listAddLong(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, (Long) value);
                    return;
                case LONG_VOID:
                    NativeEngine.listAdd(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElement(value));
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.listAddWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd(), serializeElement(value));
                    return;
                }
                default:
                    NativeEngine.listAddGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElement(value));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to add to ListState", e);
        }
    }

    @Override
    public void update(List<V> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (values.isEmpty()) {
            clear();
            return;
        }
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID: {
                    long[] arr = new long[values.size()];
                    for (int i = 0; i < arr.length; i++) { arr[i] = (Long) values.get(i); }
                    NativeEngine.listUpdateLongElements(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, arr);
                    return;
                }
                case LONG_VOID:
                    NativeEngine.listUpdate(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(values));
                    return;
                default:
                    NativeEngine.listUpdateGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(values));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update ListState", e);
        }
    }

    @Override
    public void addAll(List<V> values) {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (values.isEmpty()) {
            return;
        }
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID: {
                    long[] arr = new long[values.size()];
                    for (int i = 0; i < arr.length; i++) { arr[i] = (Long) values.get(i); }
                    NativeEngine.listAddAllLongElements(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, arr);
                    return;
                }
                case LONG_VOID:
                    NativeEngine.listAddAll(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(values));
                    return;
                default:
                    NativeEngine.listAddAllGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(values));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to addAll to ListState", e);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        // Collect list elements from source namespaces and merge into target
        N previousNamespace = currentNamespace;

        List<V> merged = null;
        for (N source : sources) {
            if (source.equals(target)) {
                continue;
            }
            setCurrentNamespace(source);
            List<V> sourceList = getInternal();
            if (sourceList != null) {
                clear();
                if (merged == null) {
                    merged = new ArrayList<>(sourceList);
                } else {
                    merged.addAll(sourceList);
                }
            }
        }

        if (merged != null) {
            setCurrentNamespace(target);
            List<V> targetList = getInternal();
            if (targetList != null) {
                merged.addAll(0, targetList);
            }
            updateInternal(merged);
        }
        setCurrentNamespace(previousNamespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<V> getInternal() {
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID: {
                    long[] arr = NativeEngine.listGetLongElements(stateHandle,
                            resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (arr == null) return null;
                    List<V> list = new ArrayList<>(arr.length);
                    for (long v : arr) { list.add((V) Long.valueOf(v)); }
                    return list;
                }
                case LONG_VOID: {
                    byte[] data = NativeEngine.listGet(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (data == null) return null;
                    return deserializeList(data);
                }
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    byte[] data = NativeEngine.listGetWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    if (data == null) return null;
                    return deserializeList(data);
                }
                default: {
                    byte[] data = NativeEngine.listGetGeneric(stateHandle, serializeKey(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    if (data == null) return null;
                    return deserializeList(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to get internal ListState", e);
        }
    }

    @Override
    public void updateInternal(List<V> valueToStore) {
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID: {
                    long[] arr = new long[valueToStore.size()];
                    for (int i = 0; i < arr.length; i++) { arr[i] = (Long) valueToStore.get(i); }
                    NativeEngine.listUpdateLongElements(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, arr);
                    return;
                }
                case LONG_VOID:
                    NativeEngine.listUpdate(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(valueToStore));
                    return;
                default:
                    NativeEngine.listUpdateGeneric(stateHandle, serializeKey(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, serializeElementList(valueToStore));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update internal ListState", e);
        }
    }

    @Override
    public void clear() {
        try {
            switch (listStrategy) {
                case LONG_ELEM_LONG_VOID:
                case LONG_VOID:
                    NativeEngine.listClear(stateHandle, resolveKeyAsLong(keyContext.currentKey), keyContext.currentKeyGroupIndex);
                    return;
                case LONG_TW: {
                    TimeWindow tw = (TimeWindow) currentNamespace;
                    NativeEngine.listClearWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                            keyContext.currentKeyGroupIndex, tw.getStart(), tw.getEnd());
                    return;
                }
                default:
                    NativeEngine.listClearGeneric(stateHandle, serializeKey(keyContext.currentKey), keyContext.currentKeyGroupIndex);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear ListState", e);
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
    public TypeSerializer<List<V>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException("Queryable state not supported by ForL0StateBackend");
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(
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

    /**
     * Serialize storage key. For non-VoidNamespace, includes namespace to isolate
     * different namespaces (e.g., TimeWindow) in the same C++ state table.
     */
    @SuppressWarnings("unchecked")
    private byte[] serializeKey(K key) throws IOException {
        keyOut.clear();
        if (!voidNamespace) {
            ((TypeSerializer<N>) namespaceSerializer).serialize(currentNamespace, keyOut);
        }
        keySerializer.serialize(key, keyOut);
        return keyOut.getCopyOfBuffer();
    }

    private byte[] serializeElement(V element) throws IOException {
        if (isRowDataElement) {
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(element);
            if (raw != null) return raw;
        }
        elemOut.clear();
        elementSerializer.serialize(element, elemOut);
        return elemOut.getCopyOfBuffer();
    }

    /** Serialize a list of elements: [count (int)] + [element bytes...]. */
    private byte[] serializeElementList(List<V> elements) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(elements.size() * 32);
        out.writeInt(elements.size());
        if (isRowDataElement) {
            for (V elem : elements) {
                Preconditions.checkNotNull(elem, "You cannot add null to a ListState.");
                byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(elem);
                if (raw != null) {
                    out.write(raw);
                } else {
                    elementSerializer.serialize(elem, out);
                }
            }
        } else {
            for (V elem : elements) {
                Preconditions.checkNotNull(elem, "You cannot add null to a ListState.");
                elementSerializer.serialize(elem, out);
            }
        }
        return out.getCopyOfBuffer();
    }

    /** Deserialize a list from: [count (int)] + [element bytes...]. */
    @SuppressWarnings("unchecked")
    private List<V> deserializeList(byte[] data) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(data);
        int count = in.readInt();
        List<V> list = new ArrayList<>(count);
        if (isRowDataElement) {
            int arity = rowDataElementAccessor.getArity();
            for (int i = 0; i < count; i++) {
                // Compat format: [4-byte size][raw BinaryRowData bytes]
                int size = in.readInt();
                byte[] raw = new byte[size];
                in.readFully(raw);
                list.add((V) RowDataKeyAccessor.wrapBinaryRowData(raw, arity));
            }
        } else {
            for (int i = 0; i < count; i++) {
                list.add(elementSerializer.deserialize(in));
            }
        }
        return list;
    }

    // ========== Factory Methods ==========

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            long stateHandle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            ForL0KeyedStateBackend<K> backend) {
        // Extract element serializer from ListSerializer
        TypeSerializer elementSerializer;
        TypeSerializer listSerializer;
        if (stateSerializer instanceof org.apache.flink.api.common.typeutils.base.ListSerializer) {
            org.apache.flink.api.common.typeutils.base.ListSerializer ls =
                    (org.apache.flink.api.common.typeutils.base.ListSerializer) stateSerializer;
            elementSerializer = ls.getElementSerializer();
            listSerializer = stateSerializer;
        } else {
            listSerializer = stateSerializer;
            elementSerializer = null; // Will fail at runtime if list operations are used
        }
        return (IS) new ForL0ListState<>(
                stateHandle,
                backend.getForL0KeyContext(),
                backend.getKeySerializer(),
                namespaceSerializer,
                elementSerializer,
                listSerializer,
                (List) stateDesc.getDefaultValue());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<SV> stateSerializer,
            IS existingState) {
        TypeSerializer elementSerializer;
        TypeSerializer listSerializer;
        if (stateSerializer instanceof org.apache.flink.api.common.typeutils.base.ListSerializer) {
            org.apache.flink.api.common.typeutils.base.ListSerializer ls =
                    (org.apache.flink.api.common.typeutils.base.ListSerializer) stateSerializer;
            elementSerializer = ls.getElementSerializer();
            listSerializer = stateSerializer;
        } else {
            listSerializer = stateSerializer;
            elementSerializer = null;
        }
        ForL0ListState state = (ForL0ListState) existingState;
        state.setNamespaceSerializer(namespaceSerializer);
        state.setValueSerializer(listSerializer);
        state.setElementSerializer(elementSerializer);
        state.setDefaultValue((List) stateDesc.getDefaultValue());
        return (IS) state;
    }

    ForL0ListState<K, N, V> setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    ForL0ListState<K, N, V> setValueSerializer(TypeSerializer<List<V>> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    ForL0ListState<K, N, V> setElementSerializer(TypeSerializer<V> elementSerializer) {
        this.elementSerializer = elementSerializer;
        return this;
    }

    ForL0ListState<K, N, V> setDefaultValue(List<V> defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
