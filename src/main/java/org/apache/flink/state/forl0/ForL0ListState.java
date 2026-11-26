package org.apache.flink.state.forl0;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryRowData;
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

    private enum ListStrategy { LONG_ELEM_LONG_VOID, LONG_VOID, LONG_TW, LONG_NS, GENERIC }
    private final ListStrategy listStrategy;
    private final NamespaceReadIndex windowIndex;
    private static final int APPEND_ELEMENT = 0, REPLACE_LIST = 1, APPEND_LIST = 2, CLEAR_LIST = 3, APPEND_ROW = 4;
    // Prepared before the element serializer can reuse key data. Only the owning task thread accesses these.
    private long windowKey, windowHeader;
    private int windowKeyLength;
    private final boolean inlineWindowKeys, emptyRowWindowKey;
    private MemorySegment serializedWindowKey;

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
        } else if (namespaceSerializer instanceof org.apache.flink.api.common.typeutils.base.LongSerializer
                && NativeEngine.listUsesLongNamespaces(stateHandle)) {
            this.listStrategy = ListStrategy.LONG_NS;
        } else {
            this.listStrategy = ListStrategy.GENERIC;
        }
        this.inlineWindowKeys = listStrategy == ListStrategy.LONG_NS && keyTypeId != TypeAnalyzer.TYPE_INT64
                && supportsInlineWindowKeys(stateHandle);
        this.emptyRowWindowKey = inlineWindowKeys && TypeAnalyzer.isStandardRowDataSerializer(keySerializer)
                && rowDataKeyAccessor != null && rowDataKeyAccessor.getArity() == 0;
        this.windowIndex = listStrategy == ListStrategy.LONG_NS && keyTypeId == TypeAnalyzer.TYPE_INT64
                ? NamespaceReadIndex.forState(stateHandle, keyContext.keyGroupRange.getStartKeyGroup(), false) : null;
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
                case LONG_NS: {
                    byte[] data = readWindowList();
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
            if (isRowDataElement) {
                addRow(value);
                return;
            }
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
                case LONG_NS: {
                    byte[] key = prepareWindowKey();
                    byte[] bytes = serializeElement(value);
                    changeWindowList(key, APPEND_ELEMENT, bytes, 0, bytes.length);
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
                case LONG_TW:
                    writeWindowList(values, false);
                    return;
                case LONG_NS:
                    changeWindowValues(values, REPLACE_LIST);
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
                case LONG_TW:
                    writeWindowList(values, true);
                    return;
                case LONG_NS:
                    changeWindowValues(values, APPEND_LIST);
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
                case LONG_NS: {
                    byte[] data = readWindowList();
                    return data == null ? null : deserializeList(data);
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
                case LONG_TW:
                    writeWindowList(valueToStore, false);
                    return;
                case LONG_NS:
                    changeWindowValues(valueToStore, REPLACE_LIST);
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
                case LONG_NS:
                    changeWindowList(prepareWindowKey(), CLEAR_LIST, null, 0, 0);
                    return;
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
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(
                    TypeAnalyzer.toBinaryRow(elementSerializer, (org.apache.flink.table.data.RowData) element));
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
                org.apache.flink.table.data.binary.BinaryRowData row = heapBinaryRow(elem);
                out.writeInt(row.getSizeInBytes());
                out.write(row.getSegments()[0].getArray(), row.getOffset(), row.getSizeInBytes());
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
            org.apache.flink.core.memory.MemorySegment segment =
                    org.apache.flink.core.memory.MemorySegmentFactory.wrap(data);
            for (int i = 0; i < count; i++) {
                // Compat format: [4-byte size][raw BinaryRowData bytes]
                int size = in.readInt();
                if (size < 0 || size > in.available()) throw new IOException("Invalid ListState row length");
                org.apache.flink.table.data.binary.BinaryRowData row =
                        new org.apache.flink.table.data.binary.BinaryRowData(arity);
                row.pointTo(segment, data.length - in.available(), size);
                in.skipBytesToRead(size);
                list.add((V) row);
            }
        } else {
            for (int i = 0; i < count; i++) {
                list.add(elementSerializer.deserialize(in));
            }
        }
        return list;
    }

    private void writeWindowList(List<V> values, boolean append) throws IOException {
        TimeWindow window = (TimeWindow) currentNamespace;
        NativeEngine.listWriteWithTW(stateHandle, resolveKeyAsLong(keyContext.currentKey),
                keyContext.currentKeyGroupIndex, window.getStart(), window.getEnd(),
                serializeElementList(values), append);
    }

    private org.apache.flink.table.data.binary.BinaryRowData heapBinaryRow(V value) {
        org.apache.flink.table.data.binary.BinaryRowData row = TypeAnalyzer.toBinaryRow(
                elementSerializer, (org.apache.flink.table.data.RowData) value);
        if (row.getSegments().length != 1 || row.getSegments()[0].getArray() == null
                || row.getOffset() > row.getSegments()[0].size() - row.getSizeInBytes()) row = row.copy();
        return row;
    }

    private static boolean supportsInlineWindowKeys(long state) {
        try {
            return NativeEngine.listSupportsInlineWindowKeys(state);
        } catch (UnsatisfiedLinkError oldLibrary) {
            return false;
        }
    }

    // This is the standard Flink four-byte size followed by all eight row-header
    // bytes. Padding, row kind and the offset of a binary slice are part of the key.
    private boolean prepareEmptyWindowRow(K key) {
        long header;
        if (key instanceof BinaryRowData) {
            BinaryRowData row = (BinaryRowData) key;
            if (row.getSizeInBytes() != 8 || row.getSegments().length != 1
                    || row.getOffset() > row.getSegments()[0].size() - 8) return false;
            header = row.getSegments()[0].getLongLittleEndian(row.getOffset());
        } else if (key instanceof GenericRowData && ((GenericRowData) key).getArity() == 0) {
            header = ((GenericRowData) key).getRowKind().toByteValue();
        } else {
            return false;
        }
        windowKey = (header << 32) | 0x08000000L;
        windowHeader = header >>> 32;
        windowKeyLength = ~12;
        return true;
    }

    private byte[] prepareWindowKey() throws IOException {
        K key = keyContext.currentKey;
        if (keyTypeId != TypeAnalyzer.TYPE_INT64) {
            if (emptyRowWindowKey && prepareEmptyWindowRow(key)) return null;
            keyOut.clear();
            keySerializer.serialize(key, keyOut);
            byte[] buffer = keyOut.getSharedBuffer();
            windowKeyLength = keyOut.length();
            if (!inlineWindowKeys || windowKeyLength > 16) return buffer;
            if (serializedWindowKey == null || serializedWindowKey.getArray() != buffer)
                serializedWindowKey = MemorySegmentFactory.wrap(buffer);
            // The serializer buffer starts at 64 bytes. Native reads only the declared length.
            windowKey = serializedWindowKey.getLongLittleEndian(0);
            windowHeader = windowKeyLength > 8 ? serializedWindowKey.getLongLittleEndian(8) : 0;
            windowKeyLength = ~windowKeyLength;
            return null;
        }
        if (isRowDataKey) {
            org.apache.flink.table.data.RowData row = (org.apache.flink.table.data.RowData) key;
            boolean missing = row.isNullAt(0);
            windowHeader = row.getRowKind().toByteValue() | (missing ? 256L : 0L);
            windowKey = missing ? 0L : rowDataKeyAccessor.extractFixedField(key, 0);
        } else {
            windowKey = (Long) key;
            windowHeader = 0;
        }
        return null;
    }

    private byte[] readWindowList() throws IOException {
        byte[] key = prepareWindowKey();
        int group = keyContext.currentKeyGroupIndex;
        long ns = (Long) currentNamespace;
        if (windowIndex != null) {
            int match = windowIndex.readWindow(group, NamespaceReadIndex.LONG, ns, 0, windowKey, windowHeader, true);
            if (match == NamespaceReadIndex.ABSENT) return null;
            if (match == NamespaceReadIndex.ROW) return windowIndex.copyCurrentRow();
        }
        return NativeEngine.listGetLongNamespace(stateHandle, group, ns, windowKey, windowHeader, key, windowKeyLength);
    }

    private void changeWindowList(byte[] key, int operation, byte[] bytes, int offset, int length) {
        NativeEngine.listChangeLongNamespace(stateHandle, keyContext.currentKeyGroupIndex,
                (Long) currentNamespace, windowKey, windowHeader, key, windowKeyLength,
                operation, bytes, offset, length);
    }

    private void changeWindowValues(List<V> values, int operation) throws IOException {
        byte[] key = prepareWindowKey();
        byte[] bytes = serializeElementList(values);
        changeWindowList(key, operation, bytes, 0, bytes.length);
    }

    private void addRow(V value) throws IOException {
        if (listStrategy == ListStrategy.LONG_NS) {
            byte[] key = prepareWindowKey();
            org.apache.flink.table.data.binary.BinaryRowData row = heapBinaryRow(value);
            changeWindowList(key, APPEND_ROW, row.getSegments()[0].getArray(), row.getOffset(), row.getSizeInBytes());
            return;
        }
        // Serialize the parent first; the element conversion can reuse its
        // serializer's scratch array. The row itself goes straight into JNI.
        byte[] key = listStrategy == ListStrategy.GENERIC ? serializeKey(keyContext.currentKey) : null;
        org.apache.flink.table.data.binary.BinaryRowData row = heapBinaryRow(value);
        int mode = listStrategy == ListStrategy.GENERIC ? 2 : listStrategy == ListStrategy.LONG_TW ? 1 : 0;
        long numeric = mode == 2 ? 0 : resolveKeyAsLong(keyContext.currentKey);
        TimeWindow window = mode == 1 ? (TimeWindow) currentNamespace : null;
        NativeEngine.listAddRowRange(stateHandle, numeric, key, keyContext.currentKeyGroupIndex,
                window == null ? 0 : window.getStart(), window == null ? 0 : window.getEnd(), mode,
                row.getSegments()[0].getArray(), row.getOffset(), row.getSizeInBytes());
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
