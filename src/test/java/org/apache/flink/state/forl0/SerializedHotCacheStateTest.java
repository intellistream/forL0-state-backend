package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SerializedHotCacheStateTest {
    private static class CountingSqlWindowSerializer extends org.apache.flink.table.runtime.operators.window.TimeWindow.Serializer {
        int serializations, deserializations;
        @Override public void serialize(org.apache.flink.table.runtime.operators.window.TimeWindow value,
                                        org.apache.flink.core.memory.DataOutputView out) throws java.io.IOException {
            serializations++; super.serialize(value, out);
        }
        @Override public org.apache.flink.table.runtime.operators.window.TimeWindow deserialize(
                org.apache.flink.core.memory.DataInputView in) throws java.io.IOException {
            deserializations++; return super.deserialize(in);
        }
    }
    private static class CountingRowSerializer extends RowDataSerializer {
        int serializations, deserializations;
        CountingRowSerializer(org.apache.flink.table.types.logical.LogicalType... types) { super(types); }
        @Override public void serialize(RowData value, org.apache.flink.core.memory.DataOutputView out) throws java.io.IOException {
            serializations++;
            super.serialize(value, out);
        }
        @Override public RowData deserialize(org.apache.flink.core.memory.DataInputView in) throws java.io.IOException {
            deserializations++;
            return super.deserialize(in);
        }
    }
    private long engine() {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        return NativeEngine.createEngine(0, 2, 2, true, 8 * 1024 * 1024,
                true, 65536, 0, 16, 0, .875, 0);
    }
    private <K, N, V> long register(long engine, int kind, TypeSerializer<K> key,
                                   TypeSerializer<N> ns, TypeSerializer<V> value) throws Exception {
        long handle = NativeEngine.registerState(engine, "test", kind,
                TypeAnalyzer.getTypeId(key), TypeAnalyzer.getTypeId(value), TypeAnalyzer.getTypeId(ns),
                TypeAnalyzer.generateStateDescriptor(key, ns, value));
        assertTrue(NativeEngine.hasSerializedCache(handle));
        return handle;
    }
    private <K> ForL0KeyContext<K> context(K key) {
        ForL0KeyContext<K> ctx = new ForL0KeyContext<>(new KeyGroupRange(0, 1), 2);
        ctx.currentKey = key;
        return ctx;
    }
    private RowDataSerializer keySerializer() { return new RowDataSerializer(new BigIntType(), new BigIntType()); }
    private RowDataSerializer rowSerializer() { return new RowDataSerializer(new BigIntType(), new VarCharType()); }
    private RowData row(long n, String text) { return GenericRowData.of(n, text == null ? null : StringData.fromString(text)); }
    private void traffic(long handle) {
        long[] stats = new long[7]; NativeEngine.getHotCacheStats(handle, stats);
        assertEquals(1, stats[0]); assertTrue(stats[1] > 0);
        assertTrue(stats[2] > 0); assertTrue(stats[4] > 0);
    }

    @Test void serializedStateHonorsTheConfiguredQuotaAbove64KiB() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 8 * 1024 * 1024,
                true, 128 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, rowSerializer());
            long[] range = NativeEngine.serializedCacheMemoryRange(handle);
            assertNotNull(range);
            assertTrue(range[1] > 64 * 1024, "The configured quota must not be silently capped at 64 KiB");
            assertTrue(range[1] <= 128 * 1024);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void sqlWindowsAvoidKeySerializationAndKeepLegacyCheckpointBytes() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer keys = new CountingRowSerializer(new BigIntType());
            CountingRowSerializer values = new CountingRowSerializer(new BigIntType(), new VarCharType());
            CountingSqlWindowSerializer windows = new CountingSqlWindowSerializer();
            long handle = register(engine, 0, keys, windows, values);
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of(1L));
            ForL0ValueState<RowData, org.apache.flink.table.runtime.operators.window.TimeWindow, RowData> state =
                    new ForL0ValueState<>(handle, ctx, keys, windows, values, null);
            org.apache.flink.table.runtime.operators.window.TimeWindow first = new org.apache.flink.table.runtime.operators.window.TimeWindow(-10, 20);
            state.setCurrentNamespace(first); state.update(row(7, "original"));
            for (int i = 0; i < 20; i++) assertEquals("original", state.value().getString(1).toString());
            state.update(state.value()); // Borrowed native input, copied synchronously.
            assertEquals(0, keys.serializations); assertEquals(0, windows.serializations);
            assertEquals(0, values.serializations); assertEquals(0, values.deserializations);
            org.apache.flink.core.memory.DataOutputSerializer keyBytes = new org.apache.flink.core.memory.DataOutputSerializer(64);
            new org.apache.flink.table.runtime.operators.window.TimeWindow.Serializer().serialize(first, keyBytes);
            new RowDataSerializer(new BigIntType()).serialize(ctx.currentKey, keyBytes);
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytes(new RowDataSerializer(new BigIntType(), new VarCharType()).toBinaryRow(row(7, "original")));
            org.apache.flink.core.memory.DataOutputSerializer legacy = new org.apache.flink.core.memory.DataOutputSerializer(128);
            legacy.writeInt(1); legacy.writeInt(keyBytes.length()); legacy.write(keyBytes.getCopyOfBuffer());
            legacy.writeInt(raw.length); legacy.write(raw);
            NativeEngine.prepareSnapshot(engine);
            assertArrayEquals(legacy.getCopyOfBuffer(), NativeEngine.writeStateKeyGroupEntries(handle, 0));
            state.update(row(9, "changed"));
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals("original", state.value().getString(1).toString());
            ctx.currentKey = GenericRowData.of((Object) null); state.update(row(2, null));
            ctx.currentKey = GenericRowData.of(0L); assertNull(state.value()); state.update(row(3, "zero"));
            ctx.currentKey = GenericRowData.of((Object) null); assertEquals(2L, state.value().getLong(0));
            state.clear(); assertNull(state.value());
            ctx.currentKey = GenericRowData.of(0L); assertEquals("zero", state.value().getString(1).toString());
            assertEquals(0, keys.serializations); assertEquals(0, windows.serializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void sqlWindowMapsUseNumericArgumentsAndPreserveIterationAndRestore() throws Exception {
        long engine = engine();
        try {
            CountingSqlWindowSerializer keys = new CountingSqlWindowSerializer(), values = new CountingSqlWindowSerializer();
            MapSerializer<org.apache.flink.table.runtime.operators.window.TimeWindow, org.apache.flink.table.runtime.operators.window.TimeWindow> maps =
                    new MapSerializer<>(keys, values);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, org.apache.flink.table.runtime.operators.window.TimeWindow, org.apache.flink.table.runtime.operators.window.TimeWindow> state =
                    new ForL0MapState<>(handle, context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, keys, values);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            org.apache.flink.table.runtime.operators.window.TimeWindow key = new org.apache.flink.table.runtime.operators.window.TimeWindow(-10, 0);
            org.apache.flink.table.runtime.operators.window.TimeWindow value = new org.apache.flink.table.runtime.operators.window.TimeWindow(-10, 30);
            state.put(key, value);
            org.apache.flink.core.memory.DataOutputSerializer legacy = new org.apache.flink.core.memory.DataOutputSerializer(64);
            legacy.writeInt(1); legacy.writeByte(0); legacy.writeLong(1L);
            legacy.writeInt(36); legacy.writeInt(1);
            legacy.writeLong(-10); legacy.writeLong(0); legacy.writeLong(-10); legacy.writeLong(30);
            NativeEngine.prepareSnapshot(engine);
            assertArrayEquals(legacy.getCopyOfBuffer(), NativeEngine.writeStateKeyGroupEntries(handle, 0));
            NativeEngine.releaseSnapshot(engine);
            assertEquals(1, NativeEngine.stateEntries(handle));
            org.apache.flink.core.memory.DataInputDeserializer storedKeys =
                    new org.apache.flink.core.memory.DataInputDeserializer(NativeEngine.getStateKeys(handle));
            assertEquals(1, storedKeys.readInt()); assertEquals(1L, storedKeys.readLong());
            for (int i = 0; i < 20; i++) { assertEquals(value, state.get(key)); assertTrue(state.contains(key)); }
            assertEquals(0, keys.serializations); assertEquals(0, values.serializations); assertEquals(0, values.deserializations);
            NativeEngine.prepareSnapshot(engine);
            state.put(key, new org.apache.flink.table.runtime.operators.window.TimeWindow(0, 99));
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(value, state.get(key));
            Iterator<Map.Entry<org.apache.flink.table.runtime.operators.window.TimeWindow, org.apache.flink.table.runtime.operators.window.TimeWindow>> it = state.iterator();
            assertEquals(value, it.next().getValue()); it.remove();
            assertNull(state.get(key)); assertTrue(state.isEmpty());
            org.apache.flink.core.memory.DataOutputSerializer user = new org.apache.flink.core.memory.DataOutputSerializer(16);
            org.apache.flink.core.memory.DataOutputSerializer val = new org.apache.flink.core.memory.DataOutputSerializer(16);
            user.writeLong(-10); user.writeLong(0); val.writeLong(-10); val.writeLong(30);
            NativeEngine.mapPut(handle, 1L, 0, user.getCopyOfBuffer(), val.getCopyOfBuffer());
            assertArrayEquals(val.getCopyOfBuffer(), NativeEngine.mapGet(handle, 1L, 0, user.getCopyOfBuffer()));
            assertTrue(NativeEngine.mapContains(handle, 1L, 0, user.getCopyOfBuffer()));
            assertEquals(value, state.get(key));
            NativeEngine.mapRemove(handle, 1L, 0, user.getCopyOfBuffer());
            assertFalse(state.contains(key));
            state.put(key, value); state.clear(); assertTrue(state.isEmpty());
            assertEquals(0, keys.serializations); assertEquals(0, values.serializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void typedWindowMapWorksWithProductionCacheDisabled() throws Exception {
        long engine = NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
        try {
            TimeWindow.Serializer windows = new TimeWindow.Serializer();
            MapSerializer<TimeWindow, TimeWindow> maps = new MapSerializer<>(windows, windows);
            long handle = NativeEngine.registerState(engine, "uncached-windows", 2, TypeAnalyzer.TYPE_INT64,
                    TypeAnalyzer.TYPE_MAP, TypeAnalyzer.TYPE_VOID_NS,
                    TypeAnalyzer.generateStateDescriptor(LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps));
            assertFalse(NativeEngine.hasSerializedCache(handle));
            ForL0MapState<Long, VoidNamespace, TimeWindow, TimeWindow> state = new ForL0MapState<>(handle,
                    context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, windows, windows);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            TimeWindow key = new TimeWindow(-10, 0), value = new TimeWindow(-10, 20);
            state.put(key, value); assertEquals(value, state.get(key)); assertTrue(state.contains(key));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.remove(key); state.put(new TimeWindow(0, 30), new TimeWindow(0, 40));
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(value, state.get(key));
            long iteratorHandle = NativeEngine.mapIteratorCreate(handle, 1L, 0);
            assertNotEquals(0, iteratorHandle);
            try {
                org.apache.flink.core.memory.DataInputDeserializer entry =
                        new org.apache.flink.core.memory.DataInputDeserializer(NativeEngine.mapIteratorNext(iteratorHandle));
                assertEquals(key, windows.deserialize(entry));
                assertEquals(value, windows.deserialize(entry));
                assertEquals(0, entry.available());
                assertNull(NativeEngine.mapIteratorNext(iteratorHandle));
            } finally { NativeEngine.mapIteratorDestroy(iteratorHandle); }
            TimeWindow otherKey = new TimeWindow(30, 40), otherValue = new TimeWindow(30, 50);
            state.put(otherKey, otherValue);
            Map<TimeWindow, TimeWindow> expected = new HashMap<>();
            expected.put(key, value); expected.put(otherKey, otherValue);
            Map<TimeWindow, TimeWindow> actual = new HashMap<>();
            for (Map.Entry<TimeWindow, TimeWindow> entry : state.entries()) actual.put(entry.getKey(), entry.getValue());
            assertEquals(expected, actual);
            Iterator<Map.Entry<TimeWindow, TimeWindow>> entries = state.iterator();
            while (entries.hasNext()) {
                Map.Entry<TimeWindow, TimeWindow> entry = entries.next();
                assertEquals(expected.remove(entry.getKey()), entry.getValue());
                entries.remove();
            }
            assertTrue(expected.isEmpty()); assertTrue(state.isEmpty());
            state.put(key, value); state.clear(); assertTrue(state.isEmpty());
            assertEquals(0, NativeEngine.stateEntries(handle));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void compositeRowsOwnPayloadAndRestoreInvalidatesCache() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = keySerializer(), values = rowSerializer();
            long handle = register(engine, 0, keys, VoidNamespaceSerializer.INSTANCE, values);
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of(1L, 2L));
            ForL0ValueState<RowData, VoidNamespace, RowData> state = new ForL0ValueState<>(
                    handle, ctx, keys, VoidNamespaceSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            GenericRowData input = (GenericRowData) row(7, "original");
            state.update(input); input.setField(1, StringData.fromString("mutated"));
            assertEquals("original", state.value().getString(1).toString());
            // Force a native main-table read, including the GenericRowData encoding path.
            NativeEngine.serializedCacheInvalidate(handle, new byte[0]);
            assertEquals("original", state.value().getString(1).toString());
            state.value();
            NativeEngine.prepareSnapshot(engine);
            state.update(row(9, null));
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            assertTrue(state.value().isNullAt(1));
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(7L, state.value().getLong(0));
            assertEquals("original", state.value().getString(1).toString());
            state.clear(); assertNull(state.value());
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void binaryRowsAndCompositeKeysDoNotAddJavaSerializationForCaching() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer keys = new CountingRowSerializer(new BigIntType(), new BigIntType());
            CountingRowSerializer values = new CountingRowSerializer(new BigIntType(), new VarCharType());
            long handle = register(engine, 0, keys, VoidNamespaceSerializer.INSTANCE, values);
            ForL0ValueState<RowData, VoidNamespace, RowData> state = new ForL0ValueState<>(handle,
                    context(GenericRowData.of(1L, 2L)), keys, VoidNamespaceSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            state.update(values.toBinaryRow(row(7, "payload")));
            for (int i = 0; i < 20; i++) assertEquals("payload", state.value().getString(1).toString());
            long[] legacy = new long[2];
            assertTrue(NativeEngine.valueGetFixedRow2GenericPtr(handle, 1L, 2L, 0, legacy));
            long result = NativeEngine.stateReadResultAddress(handle);
            assertEquals(legacy[0], org.apache.flink.core.memory.MemorySegmentBridge.readNativeLong(result));
            assertEquals(legacy[1], org.apache.flink.core.memory.MemorySegmentBridge.readNativeLong(result + 8));
            assertEquals(0, keys.serializations, "Composite keys use existing typed JNI arguments");
            assertEquals(0, values.serializations, "BinaryRowData uses its existing raw bytes");
            assertEquals(0, values.deserializations, "Cache hits return the existing native RowData view");
            state.update(row(8, "generic"));
            assertEquals("generic", state.value().getString(1).toString());
            assertEquals(0, values.serializations, "GenericRowData converts directly to raw BinaryRowData");
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void windowNamespacesAndKeyGroupsCannotAlias() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = new RowDataSerializer(new BigIntType());
            long handle = register(engine, 0, keys, new TimeWindow.Serializer(), rowSerializer());
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of(1L));
            ForL0ValueState<RowData, TimeWindow, RowData> state = new ForL0ValueState<>(
                    handle, ctx, keys, new TimeWindow.Serializer(), rowSerializer(), null);
            TimeWindow first = new TimeWindow(0, 10), second = new TimeWindow(10, 20);
            state.setCurrentNamespace(first); state.update(row(1, "first"));
            state.setCurrentNamespace(second); state.update(row(2, "second"));
            ctx.currentKeyGroupIndex = 1; state.update(row(3, "other-group"));
            ctx.currentKeyGroupIndex = 0;
            assertEquals("second", state.value().getString(1).toString());
            state.setCurrentNamespace(first); assertEquals("first", state.value().getString(1).toString());
            state.clear(); assertNull(state.value());
            state.setCurrentNamespace(second); assertEquals(2L, state.value().getLong(0));
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void longWindowNamespaceUsesSameExactCache() throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, rowSerializer());
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, context(1L),
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, rowSerializer(), null);
            state.setCurrentNamespace(10L); state.update(row(1, "a"));
            state.setCurrentNamespace(20L); state.update(row(2, "b"));
            state.setCurrentNamespace(10L); RowData first = state.value();
            state.setCurrentNamespace(20L); RowData second = state.value();
            assertNotSame(first, second);
            assertEquals("a", first.getString(1).toString());
            assertEquals("b", second.getString(1).toString());
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    private <K, V> void checkLegacyLongNamespaceEncoding(TypeSerializer<K> keys, K key,
                                                        TypeSerializer<V> values, V value) throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, keys, LongSerializer.INSTANCE, values);
            ForL0ValueState<K, Long, V> state = new ForL0ValueState<>(handle, context(key),
                    keys, LongSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(123L);
            state.update(value);
            org.apache.flink.core.memory.DataOutputSerializer keyOut = new org.apache.flink.core.memory.DataOutputSerializer(64);
            LongSerializer.INSTANCE.serialize(123L, keyOut);
            keys.serialize(key, keyOut);
            org.apache.flink.core.memory.DataOutputSerializer valueOut = new org.apache.flink.core.memory.DataOutputSerializer(64);
            values.serialize(value, valueOut);
            byte[] valueBytes = valueOut.getCopyOfBuffer();
            // Main's BinaryRowData path stores raw rows without the serializer length.
            if (values instanceof RowDataSerializer) valueBytes = Arrays.copyOfRange(valueBytes, 4, valueBytes.length);
            org.apache.flink.core.memory.DataOutputSerializer legacy = new org.apache.flink.core.memory.DataOutputSerializer(128);
            legacy.writeInt(1);
            legacy.writeInt(keyOut.length()); legacy.write(keyOut.getCopyOfBuffer());
            legacy.writeInt(valueBytes.length); legacy.write(valueBytes);
            NativeEngine.prepareSnapshot(engine);
            byte[] actual = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo<?, ?> meta =
                    new org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo<>(
                            org.apache.flink.api.common.state.StateDescriptor.Type.VALUE, "test", LongSerializer.INSTANCE, values);
            try (ForL0KeyValueStateIterator iterator = new ForL0KeyValueStateIterator(new KeyGroupRange(0, 1), 2,
                    keys, java.util.Collections.singletonMap(StateUID.of("test",
                    org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.KEY_VALUE), 0),
                    java.util.Collections.singletonMap("test", handle), java.util.Collections.singletonMap("test", meta),
                    java.util.Collections.emptyMap())) {
                assertTrue(iterator.isValid());
                org.apache.flink.core.memory.DataOutputSerializer canonical = new org.apache.flink.core.memory.DataOutputSerializer(64);
                canonical.writeByte(0); keys.serialize(key, canonical); LongSerializer.INSTANCE.serialize(123L, canonical);
                assertArrayEquals(canonical.getCopyOfBuffer(), iterator.key());
                assertArrayEquals(valueBytes, iterator.value());
                iterator.next(); assertFalse(iterator.isValid());
            }
            NativeEngine.releaseSnapshot(engine);
            assertArrayEquals(legacy.getCopyOfBuffer(), actual, "Typed namespaces preserve main's checkpoint bytes");
            assertNotNull(state.value());
            state.clear(); assertNull(state.value());
            NativeEngine.readStateKeyGroupEntries(handle, 0, legacy.getCopyOfBuffer());
            V restored = state.value();
            org.apache.flink.core.memory.DataOutputSerializer restoredOut = new org.apache.flink.core.memory.DataOutputSerializer(64);
            values.serialize(restored, restoredOut);
            assertArrayEquals(valueOut.getCopyOfBuffer(), restoredOut.getCopyOfBuffer());
            state.value();
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericLongNamespacesKeepLegacyCheckpointEncoding() throws Exception {
        checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, LongSerializer.INSTANCE, Long.MIN_VALUE);
        checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, IntSerializer.INSTANCE, -17);
        checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, DoubleSerializer.INSTANCE, -0.25d);
    }

    @Test void rowLongNamespacesPreserveNullsRowKindAndLegacyCheckpointEncoding() throws Exception {
        RowDataSerializer keys = new RowDataSerializer(new BigIntType());
        RowDataSerializer values = new RowDataSerializer(new BigIntType());
        GenericRowData nullable = GenericRowData.of((Object) null);
        nullable.setRowKind(org.apache.flink.types.RowKind.UPDATE_AFTER);
        checkLegacyLongNamespaceEncoding(keys, GenericRowData.of(-42L), values, nullable);
        GenericRowData nullKey = GenericRowData.of((Object) null);
        nullKey.setRowKind(org.apache.flink.types.RowKind.UPDATE_BEFORE);
        checkLegacyLongNamespaceEncoding(keys, nullKey, values, GenericRowData.of(9L));
        checkLegacyLongNamespaceEncoding(keys, GenericRowData.of(-42L), rowSerializer(), row(7L, "payload"));
    }

    @Test void nullNumericRowKeyCannotAliasZeroInLongNamespace() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = new RowDataSerializer(new BigIntType());
            long handle = register(engine, 0, keys, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of((Object) null));
            ForL0ValueState<RowData, Long, Long> state = new ForL0ValueState<>(handle, ctx,
                    keys, LongSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(1L); state.update(7L);
            ctx.currentKey = GenericRowData.of(0L); state.update(8L);
            assertEquals(8L, state.value());
            ctx.currentKey = GenericRowData.of((Object) null); assertEquals(7L, state.value());
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear(); assertNull(state.value());
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(7L, state.value());
            ctx.currentKey = GenericRowData.of(0L); assertEquals(8L, state.value());
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void longNamespacePointerReadHonorsGroupsAndRestoreWithAndWithoutCache() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (boolean cached : new boolean[]{false, true}) {
            long engine = cached ? engine() : NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer values = rowSerializer();
                long handle = NativeEngine.registerState(engine, "long-result", 0, TypeAnalyzer.TYPE_INT64,
                        TypeAnalyzer.getTypeId(values), TypeAnalyzer.TYPE_INT64,
                        TypeAnalyzer.generateStateDescriptor(LongSerializer.INSTANCE, LongSerializer.INSTANCE, values));
                ForL0KeyContext<Long> ctx = context(Long.MIN_VALUE);
                ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, ctx,
                        LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
                state.setCurrentNamespace(Long.MAX_VALUE);
                state.update(row(7L, "original"));
                ctx.currentKeyGroupIndex = 1; state.update(row(9L, "other-group"));
                ctx.currentKeyGroupIndex = 0;
                assertEquals("original", state.value().getString(1).toString());
                NativeEngine.prepareSnapshot(engine);
                byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
                state.update(row(11L, "mutated"));
                NativeEngine.releaseSnapshot(engine);
                NativeEngine.readKeyGroupData(engine, 0, snapshot);
                assertEquals("original", state.value().getString(1).toString());
                state.clear();
                assertFalse(NativeEngine.valueGetBlobPtrLongNs(handle, Long.MIN_VALUE, 0, Long.MAX_VALUE, 0));
                assertNull(state.value());
                ctx.currentKeyGroupIndex = 1;
                assertEquals("other-group", state.value().getString(1).toString());
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void longNamespaceRowsUseArrayRangesAndBorrowedNativeInputWithoutSerialization() throws Exception {
        long engine = engine();
        org.apache.flink.core.memory.MemorySegment direct = null;
        try {
            CountingRowSerializer values = new CountingRowSerializer(new BigIntType(), new VarCharType());
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, values);
            ForL0KeyContext<Long> ctx = context(1L);
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(10L);
            org.apache.flink.table.data.binary.BinaryRowData original = values.toBinaryRow(row(1L, "range"));
            int size = original.getSizeInBytes();
            byte[] padded = new byte[size + 32];
            original.getSegments()[0].get(original.getOffset(), padded, 16, size);
            org.apache.flink.table.data.binary.BinaryRowData ranged = new org.apache.flink.table.data.binary.BinaryRowData(2);
            ranged.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(padded), 16, size);
            state.update(ranged); Arrays.fill(padded, (byte) 0);
            assertEquals("range", state.value().getString(1).toString());
            ctx.currentKey = 3L; state.update(row(4L, "generic"));
            assertEquals("generic", state.value().getString(1).toString());
            original = values.toBinaryRow(row(5L, "segmented"));
            size = original.getSizeInBytes();
            org.apache.flink.core.memory.MemorySegment[] split = new org.apache.flink.core.memory.MemorySegment[(size + 15) / 16];
            for (int i = 0; i < split.length; i++) {
                byte[] part = new byte[16];
                original.getSegments()[0].get(original.getOffset() + i * 16, part, 0, Math.min(16, size - i * 16));
                split[i] = org.apache.flink.core.memory.MemorySegmentFactory.wrap(part);
            }
            ranged.pointTo(split, 0, size);
            ctx.currentKey = 4L; state.update(ranged);
            assertEquals("segmented", state.value().getString(1).toString());
            ctx.currentKey = 1L;
            NativeEngine.prepareSnapshot(engine);
            // Read from the borrowed cache view and synchronously write to a
            // different key: the JNI owner argument pins this direct memory.
            RowData borrowed = state.value();
            ctx.currentKey = 2L; state.update(borrowed);
            assertEquals("range", state.value().getString(1).toString());
            original = values.toBinaryRow(row(3L, "direct"));
            size = original.getSizeInBytes();
            direct = org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledOffHeapMemory(size + 8);
            original.getSegments()[0].copyTo(original.getOffset(), direct, 8, size);
            ranged.pointTo(direct, 8, size); state.update(ranged);
            direct.free(); direct = null;
            assertEquals("direct", state.value().getString(1).toString());
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear();
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            ctx.currentKey = 1L; assertEquals("range", state.value().getString(1).toString());
            ctx.currentKey = 2L; assertNull(state.value());
            assertEquals(0, values.serializations);
            assertEquals(0, values.deserializations);
            traffic(handle);
        } finally {
            if (direct != null) direct.free();
            NativeEngine.destroyEngine(engine);
        }
    }

    @Test void mapListsIterationRemovalAndClearStayConsistent() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = keySerializer(), rows = rowSerializer();
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, keys, LongSerializer.INSTANCE, maps);
            ForL0MapState<RowData, Long, Long, List<RowData>> state = new ForL0MapState<>(handle,
                    context(GenericRowData.of(1L, 2L)), keys, LongSerializer.INSTANCE, maps, LongSerializer.INSTANCE, lists);
            state.setCurrentNamespace(10L);
            state.put(1L, Arrays.asList(row(1, "one"), row(2, null)));
            state.put(2L, Arrays.asList(row(3, "two")));
            assertEquals(1, state.get(1L).get(0).getLong(0));
            state.get(1L); state.entries(); state.entries();
            Iterator<Map.Entry<Long, List<RowData>>> it = state.iterator();
            Long removed = it.next().getKey(); it.remove();
            assertFalse(state.contains(removed)); assertNull(state.get(removed));
            int size = 0; for (Map.Entry<Long, List<RowData>> ignored : state.entries()) size++;
            assertEquals(1, size);
            state.setCurrentNamespace(20L); state.put(5L, Arrays.asList(row(5, "other")));
            state.setCurrentNamespace(10L); state.clear(); assertTrue(state.isEmpty());
            state.setCurrentNamespace(20L); assertEquals(5L, state.get(5L).get(0).getLong(0));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.put(5L, Arrays.asList(row(99, "changed")));
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(5L, state.get(5L).get(0).getLong(0));
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void listCacheAppendsOnlyNewElementsAndRestoresOwnedRows() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = keySerializer();
            CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            long handle = register(engine, 1, keys, LongSerializer.INSTANCE, lists);
            ForL0ListState<RowData, Long, RowData> state = new ForL0ListState<>(handle,
                    context(GenericRowData.of(1L, 2L)), keys, LongSerializer.INSTANCE, rows, lists, null);
            state.setCurrentNamespace(10L);
            state.add(row(1, "one"));
            long[] before = new long[7]; NativeEngine.getHotCacheStats(handle, before);
            state.add(row(2, null));
            long[] after = new long[7]; NativeEngine.getHotCacheStats(handle, after);
            assertEquals(1, after[4] - before[4], "Append writes only the new element");
            state.addAll(Arrays.asList(row(3, "three"), row(4, "four")));
            List<RowData> retained = state.getInternal();
            assertEquals(4, retained.size());
            assertEquals("one", retained.get(0).getString(1).toString());
            assertTrue(retained.get(1).isNullAt(1));
            assertEquals(4, state.getInternal().get(3).getLong(0));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.update(Arrays.asList(row(9, "new")));
            NativeEngine.releaseSnapshot(engine);
            assertEquals(1, state.getInternal().size());
            assertEquals("one", retained.get(0).getString(1).toString());
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(4, state.getInternal().size());
            state.setCurrentNamespace(20L); state.add(row(5, "other"));
            state.mergeNamespaces(10L, Arrays.asList(20L));
            state.setCurrentNamespace(10L); assertEquals(5, state.getInternal().size());
            state.setCurrentNamespace(20L); assertNull(state.getInternal());
            state.setCurrentNamespace(10L); state.clear(); assertNull(state.getInternal());
            assertEquals(0, rows.serializations); assertEquals(0, rows.deserializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericAndTimeWindowListMutationsUseTheSameCacheIdentity() throws Exception {
        for (boolean windowed : new boolean[]{false, true}) {
            long engine = engine();
            try {
                if (windowed) checkLongList(engine, new TimeWindow.Serializer(), new TimeWindow(10, 20));
                else checkLongList(engine, VoidNamespaceSerializer.INSTANCE, VoidNamespace.INSTANCE);
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void listReadMissesKeepTheCachedTailAndSmallListsCanWarm() throws Exception {
        long engine = engine();
        try {
            ListSerializer<Long> values = new ListSerializer<>(LongSerializer.INSTANCE);
            long handle = register(engine, 1, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, values);
            ForL0ListState<Long, VoidNamespace, Long> state = new ForL0ListState<>(handle, context(1L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            List<Long> input = new java.util.ArrayList<>();
            for (long i = 0; i < 1200; ++i) input.add(i);
            state.addAll(input);
            long[] before = new long[7]; NativeEngine.getHotCacheStats(handle, before);
            assertEquals(input, state.getInternal());
            long[] after = new long[7]; NativeEngine.getHotCacheStats(handle, after);
            assertEquals(before[4], after[4], "Oversized lists are not refilled during a scan");
            assertTrue(after[2] - before[2] > 600, "Cold prefix reads retain the resident tail");
            state.update(Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear(); NativeEngine.readKeyGroupData(engine, 0, snapshot);
            NativeEngine.getHotCacheStats(handle, before);
            List<Long> expected = Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE);
            assertEquals(expected, state.getInternal());
            NativeEngine.getHotCacheStats(handle, after);
            assertEquals(4, after[4] - before[4], "A small restored list warms after the read");
            NativeEngine.getHotCacheStats(handle, before);
            assertEquals(expected, state.getInternal());
            NativeEngine.getHotCacheStats(handle, after);
            assertEquals(4, after[2] - before[2]);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    private <N> void checkLongList(long engine, TypeSerializer<N> nsSerializer, N namespace) throws Exception {
        ListSerializer<Long> values = new ListSerializer<>(LongSerializer.INSTANCE);
        long handle = register(engine, 1, LongSerializer.INSTANCE, nsSerializer, values);
        ForL0ListState<Long, N, Long> state = new ForL0ListState<>(handle, context(1L),
                LongSerializer.INSTANCE, nsSerializer, LongSerializer.INSTANCE, values, null);
        state.setCurrentNamespace(namespace);
        state.add(1L); state.addAll(Arrays.asList(2L, 3L));
        assertEquals(Arrays.asList(1L, 2L, 3L), state.getInternal());
        state.update(Arrays.asList(4L, 5L));
        assertEquals(Arrays.asList(4L, 5L), state.getInternal());
        NativeEngine.prepareSnapshot(engine);
        byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
        state.clear(); assertNull(state.getInternal());
        NativeEngine.releaseSnapshot(engine);
        NativeEngine.readKeyGroupData(engine, 0, snapshot);
        assertEquals(Arrays.asList(4L, 5L), state.getInternal());
        state.getInternal(); traffic(handle);
    }

    @Test void rowListRangesWorkWithNumericKeysAndWindowNamespaces() throws Exception {
        long engine = engine();
        try { checkRowList(engine, VoidNamespaceSerializer.INSTANCE, VoidNamespace.INSTANCE); }
        finally { NativeEngine.destroyEngine(engine); }
        engine = engine();
        try { checkRowList(engine, new TimeWindow.Serializer(), new TimeWindow(10, 20)); }
        finally { NativeEngine.destroyEngine(engine); }
    }

    private <N> void checkRowList(long engine, TypeSerializer<N> nsSerializer, N namespace) throws Exception {
        CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
        ListSerializer<RowData> lists = new ListSerializer<>(rows);
        long handle = register(engine, 1, LongSerializer.INSTANCE, nsSerializer, lists);
        ForL0ListState<Long, N, RowData> state = new ForL0ListState<>(handle, context(1L),
                LongSerializer.INSTANCE, nsSerializer, rows, lists, null);
        state.setCurrentNamespace(namespace);
        org.apache.flink.table.data.binary.BinaryRowData raw = TypeAnalyzer.toBinaryRow(rows, row(1, "offset"));
        byte[] array = new byte[raw.getSizeInBytes() + 20];
        raw.getSegments()[0].get(raw.getOffset(), array, 11, raw.getSizeInBytes());
        org.apache.flink.table.data.binary.BinaryRowData ranged = new org.apache.flink.table.data.binary.BinaryRowData(2);
        ranged.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(array), 11, raw.getSizeInBytes());
        state.add(ranged); Arrays.fill(array, (byte) 0);
        state.add(row(2, null)); state.addAll(Arrays.asList(row(3, "three")));
        List<RowData> retained = state.getInternal();
        assertEquals("offset", retained.get(0).getString(1).toString());
        assertTrue(retained.get(1).isNullAt(1)); assertEquals(3, retained.get(2).getLong(0));
        NativeEngine.prepareSnapshot(engine);
        byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
        state.update(Arrays.asList(row(9, "new")));
        NativeEngine.releaseSnapshot(engine);
        assertEquals(1, state.getInternal().size());
        NativeEngine.readKeyGroupData(engine, 0, snapshot);
        assertEquals(3, state.getInternal().size());
        assertEquals("offset", retained.get(0).getString(1).toString());
        assertEquals(0, rows.serializations); assertEquals(0, rows.deserializations);
        traffic(handle);
    }

    @Test void numericMapCachesInitialWritesAndKeepsOtherEntriesOnMutation() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, Long> maps = new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, Long> state = new ForL0MapState<>(handle,
                    context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            state.put(1L, 11L); state.put(2L, 22L);
            long[] before = new long[7]; NativeEngine.getHotCacheStats(handle, before);
            assertEquals(2, before[4], "Initial inserts immediately write owned cache payloads");
            assertEquals(11L, state.get(1L));
            state.put(2L, 23L); assertEquals(11L, state.get(1L));
            state.remove(2L); assertEquals(11L, state.get(1L)); assertNull(state.get(2L));
            long[] after = new long[7]; NativeEngine.getHotCacheStats(handle, after);
            assertEquals(3, after[2] - before[2], "Mutating one entry retains unrelated cached entries");
            state.clear(); assertNull(state.get(1L));
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void rowKeyIntegerMapsAvoidTemporarySerializationAndKeepCheckpointBytes() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            MapSerializer<RowData, Integer> maps = new MapSerializer<>(rows, IntSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle,
                    context(7L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, rows, IntSerializer.INSTANCE);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            RowData first = row(1, "one"), second = row(2, null);
            assertNull(state.get(first));
            state.put(first, Integer.MIN_VALUE); state.put(second, 0);
            assertEquals(Integer.MIN_VALUE, state.get(first)); assertEquals(0, state.get(second));
            // Legacy per-entry JNI and bulk iteration see exactly the same map.
            byte[] keyBytes = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(TypeAnalyzer.toBinaryRow(rows, first));
            assertArrayEquals(new byte[]{(byte) 0x80, 0, 0, 0}, NativeEngine.mapGet(handle, 7, 0, keyBytes));
            int count = 0; for (Map.Entry<RowData, Integer> ignored : state.entries()) count++;
            assertEquals(2, count);
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.put(first, Integer.MAX_VALUE);
            NativeEngine.releaseSnapshot(engine);
            assertEquals(Integer.MAX_VALUE, state.get(first));
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(Integer.MIN_VALUE, state.get(first));
            state.remove(first); assertNull(state.get(first)); assertEquals(0, state.get(second));
            state.put(first, 7); assertEquals(7, state.get(first));
            state.clear(); assertNull(state.get(second));
            assertEquals(0, rows.serializations); assertEquals(0, rows.deserializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void mapValueRangesExcludeSpareBufferCapacityAndOwnTheirBytes() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            MapSerializer<Long, RowData> maps = new MapSerializer<>(LongSerializer.INSTANCE, rows);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, RowData> state = new ForL0MapState<>(handle, context(1L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, rows);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            char[] large = new char[4096]; Arrays.fill(large, 'x');
            state.put(1L, row(1, new String(large)));
            RowData retained = state.get(1L);
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.put(1L, row(2, "small"));
            NativeEngine.releaseSnapshot(engine);
            RowData expected = row(2, "small");
            byte[] expectedBytes = RowDataKeyAccessor.extractBinaryRowDataBytesCompat(TypeAnalyzer.toBinaryRow(rows, expected));
            assertArrayEquals(expectedBytes, NativeEngine.mapGetLongBytes(handle, 1, 0, 1));
            state.put(2L, row(3, null));
            assertEquals("small", state.get(1L).getString(1).toString());
            assertTrue(state.get(2L).isNullAt(1));
            assertEquals(4096, retained.getString(1).toString().length());
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(4096, state.get(1L).getString(1).toString().length());
            assertEquals(0, rows.serializations); assertEquals(0, rows.deserializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test void rowKeyTupleMapsRestoreFixedWidthValuesAndRespectArrayOffsets() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            org.apache.flink.api.java.typeutils.runtime.TupleSerializer<org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>> pairs =
                    new org.apache.flink.api.java.typeutils.runtime.TupleSerializer((Class) org.apache.flink.api.java.tuple.Tuple2.class,
                            new TypeSerializer[]{IntSerializer.INSTANCE, IntSerializer.INSTANCE});
            MapSerializer<RowData, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>> maps = new MapSerializer<>(rows, pairs);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, RowData, org.apache.flink.api.java.tuple.Tuple2<Integer, Integer>> state =
                    new ForL0MapState<>(handle, context(7L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, rows, pairs);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            org.apache.flink.table.data.binary.BinaryRowData original = TypeAnalyzer.toBinaryRow(rows, row(1, "offset"));
            byte[] array = new byte[original.getSizeInBytes() + 20];
            original.getSegments()[0].get(original.getOffset(), array, 11, original.getSizeInBytes());
            org.apache.flink.table.data.binary.BinaryRowData key = new org.apache.flink.table.data.binary.BinaryRowData(2);
            key.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(array), 11, original.getSizeInBytes());
            org.apache.flink.api.java.tuple.Tuple2<Integer, Integer> value = org.apache.flink.api.java.tuple.Tuple2.of(3, 4);
            state.put(key, value); assertEquals(value, state.get(row(1, "offset")));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            state.clear(); NativeEngine.releaseSnapshot(engine);
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(value, state.get(key));
            state.remove(key); assertNull(state.get(key));
            assertEquals(0, rows.serializations); assertEquals(0, rows.deserializations);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void timeWindowMapUsesTheCorrectContainerAndRestoresBothWindows() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, Long> maps = new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            TimeWindow.Serializer windows = new TimeWindow.Serializer();
            long handle = register(engine, 2, LongSerializer.INSTANCE, windows, maps);
            ForL0MapState<Long, TimeWindow, Long, Long> state = new ForL0MapState<>(handle,
                    context(1L), LongSerializer.INSTANCE, windows, maps, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            TimeWindow first = new TimeWindow(0, 10), second = new TimeWindow(10, 20);
            state.setCurrentNamespace(first);
            for (long i = 0; i < 2000; i++) state.put(i, i + 1);
            assertEquals(2000L, state.get(1999L));
            state.setCurrentNamespace(second); state.put(1999L, 42L);
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear();
            NativeEngine.readKeyGroupData(engine, 0, snapshot);
            assertEquals(42L, state.get(1999L));
            state.setCurrentNamespace(first); assertEquals(2000L, state.get(1999L));
            int count = 0; for (Map.Entry<Long, Long> ignored : state.entries()) count++;
            assertEquals(2000, count);
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }
}
