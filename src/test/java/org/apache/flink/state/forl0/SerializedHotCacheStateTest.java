package org.apache.flink.state.forl0;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
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
    private static long windowControlCount(ByteBuffer controls, int field) {
        long count = 0;
        for (int offset = field; offset < controls.capacity(); offset += NamespaceReadIndex.CONTROL_BYTES) {
            count += controls.getLong(offset);
        }
        return count;
    }

    private long engine() {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        return NativeEngine.createEngine(0, 2, 2, true, 8 * 1024 * 1024,
                true, 65536, 0, 16, 0, .875, 0);
    }
    private <K, N, V> long register(long engine, int kind, TypeSerializer<K> key,
                                   TypeSerializer<N> ns, TypeSerializer<V> value) throws Exception {
        return NativeEngine.registerState(engine, "test", kind,
                TypeAnalyzer.getTypeId(key), TypeAnalyzer.getTypeId(value), TypeAnalyzer.getTypeId(ns),
                TypeAnalyzer.generateStateDescriptor(key, ns, value));
    }
    @Test
    void overlappingCheckpointAndSavepointKeepTheirOwnCutAcrossRegistrationAndDispose() throws Exception {
        long engine = engine();
        long restored = engine();
        ForL0NativeSnapshots snapshots = new ForL0NativeSnapshots(engine, new KeyGroupRange(0, 1));
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    LongSerializer.INSTANCE);
            long restoredHandle = register(restored, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    LongSerializer.INSTANCE);
            java.util.Collection<Long> handles = java.util.Collections.singletonList(handle);
            NativeEngine.valuePutLongLong(handle, 7, 0, 10);
            NativeEngine.valuePutLongLong(handle, 8, 1, 11);
            ForL0NativeSnapshots.Snapshot first = snapshots.prepare(false, handles);
            byte[] group0 = first.checkpointGroup(0);

            NativeEngine.valuePutLongLong(handle, 8, 1, 12);
            ForL0NativeSnapshots.Snapshot second = snapshots.prepare(true, handles);
            NativeEngine.valuePutLongLong(handle, 8, 1, 13);
            snapshots.detach();
            NativeEngine.registerState(engine, "later", 0, TypeAnalyzer.TYPE_INT64, TypeAnalyzer.TYPE_INT64,
                    TypeAnalyzer.getTypeId(VoidNamespaceSerializer.INSTANCE), TypeAnalyzer.generateStateDescriptor(
                            LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE));

            ForL0NativeSnapshots.Snapshot third = snapshots.prepare(false, handles);
            NativeEngine.valuePutLongLong(handle, 8, 1, 14);
            NativeEngine.readKeyGroupData(restored, 0, group0);
            NativeEngine.readKeyGroupData(restored, 1, first.checkpointGroup(1));
            assertEquals(10L, NativeEngine.valueGetLongLong(restoredHandle, 7, 0));
            assertEquals(11L, NativeEngine.valueGetLongLong(restoredHandle, 8, 1));
            first.release();
            first.release();

            third.checkpointGroup(0);
            NativeEngine.readKeyGroupData(restored, 1, third.checkpointGroup(1));
            assertEquals(13L, NativeEngine.valueGetLongLong(restoredHandle, 8, 1));
            snapshots.dispose();
            assertThrows(IllegalStateException.class, () -> snapshots.prepare(false, handles));
            NativeEngine.readStateKeyGroupEntries(restoredHandle, 1, second.stateGroup(handle, 1));
            assertEquals(12L, NativeEngine.valueGetLongLong(restoredHandle, 8, 1));
            second.release();
            third.release();
        } finally {
            snapshots.dispose();
            NativeEngine.destroyEngine(restored);
        }
    }

    @Test
    void checkpointReturnsOwnedBytesAndReleasesOnlyTheConsumedGroup() throws Exception {
        long engine = engine();
        long restored = engine();
        ForL0NativeSnapshots snapshots = new ForL0NativeSnapshots(engine, new KeyGroupRange(0, 1));
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    LongSerializer.INSTANCE);
            long target = register(restored, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    LongSerializer.INSTANCE);
            NativeEngine.valuePutLongLong(handle, 1, 0, 10);
            NativeEngine.valuePutLongLong(handle, 2, 1, 20);
            ForL0NativeSnapshots.Snapshot snapshot = snapshots.prepare(false,
                    java.util.Collections.singletonList(handle));
            NativeEngine.valuePutLongLong(handle, 1, 0, 11);
            NativeEngine.valuePutLongLong(handle, 2, 1, 21);
            assertThrows(IllegalStateException.class, () -> snapshot.checkpointGroup(1));
            byte[] first = snapshot.checkpointGroup(0);
            NativeEngine.valuePutLongLong(handle, 1, 0, 12);
            NativeEngine.readKeyGroupData(restored, 0, NativeEngine.writeKeyGroupData(engine, 0));
            assertEquals(12L, NativeEngine.valueGetLongLong(target, 1, 0));
            NativeEngine.readKeyGroupData(restored, 1, NativeEngine.writeKeyGroupData(engine, 1));
            assertEquals(20L, NativeEngine.valueGetLongLong(target, 2, 1));
            byte[] second = snapshot.checkpointGroup(1);
            assertThrows(IllegalStateException.class, () -> snapshot.checkpointGroup(2));
            snapshot.release();
            NativeEngine.readKeyGroupData(restored, 0, first);
            NativeEngine.readKeyGroupData(restored, 1, second);
            assertEquals(10L, NativeEngine.valueGetLongLong(target, 1, 0));
            assertEquals(20L, NativeEngine.valueGetLongLong(target, 2, 1));
        } finally {
            snapshots.dispose();
            NativeEngine.destroyEngine(restored);
        }
    }

    @Test
    void cancelledSnapshotReadersCannotAccessADisposedEngine() throws Exception {
        long engine = engine();
        ForL0NativeSnapshots snapshots = new ForL0NativeSnapshots(engine, new KeyGroupRange(0, 1));
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    LongSerializer.INSTANCE);
            java.util.Collection<Long> handles = java.util.Collections.singletonList(handle);
            ForL0NativeSnapshots.Snapshot cancelled = snapshots.prepare(false, handles);
            cancelled.release(); // Cancellation before the async supplier starts.
            NativeEngine.valuePutLongLong(handle, 11, 0, 1);
            ForL0NativeSnapshots.Snapshot live = snapshots.prepare(true, handles);
            java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.Future<Boolean> reader = executor.submit(() -> {
                assertNotNull(live.stateGroup(handle, 0));
                entered.countDown();
                for (;;) {
                    try { live.stateGroup(handle, 0); }
                    catch (IllegalStateException disposed) { return true; }
                }
            });
            assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
            snapshots.dispose();
            assertTrue(reader.get(10, java.util.concurrent.TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> cancelled.checkpointGroup(0));
            live.release();
        } finally {
            snapshots.dispose();
            executor.shutdownNow();
        }
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
        long[] stats = new long[7];
        NativeEngine.getHotCacheStats(handle, stats);
        assertEquals(1, stats[0]);
        assertTrue(stats[1] > 0);
    }

    @Test void serializedStateHonorsTheConfiguredQuotaAbove64KiB() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 8 * 1024 * 1024,
                true, 128 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, rowSerializer());
            long[] range = NativeEngine.stateCacheMemoryRange(handle);
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

    @Test void singleRowWindowGrowsUnderSnapshotAndRejectsEveryOtherKey() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = engine();
        try {
            LongSerializer longs = LongSerializer.INSTANCE;
            RowDataSerializer rows = new RowDataSerializer(new BigIntType());
            long handle = register(engine, 0, longs, longs, rows);
            ForL0KeyContext<Long> keys = context(7L);
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, keys, longs, longs, rows, null);
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (long ns = 0; ns < 24; ++ns) {
                state.setCurrentNamespace(ns);
                state.update(GenericRowData.of(ns)); assertEquals(ns, state.value().getLong(0)); state.clear();
            }
            state.setCurrentNamespace(200L); state.update(GenericRowData.of(71L));
            long direct = 0;
            for (int i = 0; i < 160; ++i) {
                keys.currentKey = 7L; assertEquals(71, state.value().getLong(0));
                keys.currentKey = 8L + i; assertNull(state.value());
            }
            for (int cell = 0; cell < 4 * NamespaceReadIndex.CONTROL_BYTES; cell += NamespaceReadIndex.CONTROL_BYTES) {
                if (views.getLong(cell + 24) == 200 && views.getLong(cell + 48) != 0) {
                    assertEquals(0, views.getLong(cell + 8)); direct += views.getLong(cell + 64);
                }
            }
            assertTrue(direct > 100);
            NativeEngine.prepareSnapshot(engine);
            keys.currentKey = 8L; state.update(GenericRowData.of(88L));
            keys.currentKey = 7L; state.update(GenericRowData.of(99L));
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            assertEquals(99, state.value().getLong(0)); state.clear();
            keys.currentKey = 8L; assertEquals(88, state.value().getLong(0)); state.clear();
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            assertNull(state.value()); keys.currentKey = 7L; assertEquals(71, state.value().getLong(0));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void wideJoinKeysRestoreAcrossBothTableLayoutsAndKeepIndependentEntries() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long cached = engine(), ordinary = NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer rows = new RowDataSerializer(new BigIntType(), new BigIntType(), new VarCharType(),
                    new BigIntType(), new VarCharType());
            MapSerializer<RowData, Integer> maps = new MapSerializer<>(rows, IntSerializer.INSTANCE);
            LongSerializer longs = LongSerializer.INSTANCE;
            VoidNamespaceSerializer ns = VoidNamespaceSerializer.INSTANCE;
            long current = register(cached, 2, longs, ns, maps);
            byte[] legacy = TypeAnalyzer.generateStateDescriptor(longs, ns, maps);
            int keyFlags = legacy.length - 10;
            assertEquals(5, legacy[keyFlags]); legacy[keyFlags] &= ~4;
            long old = NativeEngine.registerState(ordinary, "legacy", 2, TypeAnalyzer.getTypeId(longs),
                    TypeAnalyzer.getTypeId(maps), TypeAnalyzer.getTypeId(ns), legacy);
            ForL0KeyContext<Long> keys = context(42L);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(
                    current, keys, longs, ns, maps, rows, IntSerializer.INSTANCE);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> prior = new ForL0MapState<>(
                    old, keys, longs, ns, maps, rows, IntSerializer.INSTANCE);
            BinaryRowData[] inner = new BinaryRowData[24];
            for (int i = 0; i < inner.length; ++i) {
                inner[i] = rows.toBinaryRow(GenericRowData.of(98L, (long) i, StringData.fromString("wide".repeat(38)),
                        -7L, i % 3 == 0 ? null : StringData.fromString("z"))).copy();
                prior.put(inner[i], -20 - i);
            }
            NativeEngine.prepareSnapshot(ordinary);
            byte[] oldBytes = NativeEngine.writeStateKeyGroupEntries(old, 0);
            NativeEngine.releaseSnapshot(ordinary);
            NativeEngine.readStateKeyGroupEntries(current, 0, oldBytes);
            for (int i = 0; i < 200; ++i) {
                int j = i % inner.length;
                assertEquals(-20 - j, state.get(inner[j]));
                state.put(inner[j], -20 - j);
            }
            java.nio.ByteBuffer controls = NativeEngine.residentControlViews(current).order(java.nio.ByteOrder.nativeOrder());
            assertEquals(249L, controls.getLong(56) >>> 56);
            assertTrue(controls.getLong(64) > 0);
            java.util.HashSet<Integer> counts = new java.util.HashSet<>();
            for (Integer count : state.values()) counts.add(count);
            assertEquals(24, counts.size());
            int count = 0; for (RowData k : state.keys()) { assertEquals(-20 - k.getLong(1), state.get(k).longValue()); count++; }
            assertEquals(inner.length, count);
            Iterable<Map.Entry<RowData, Integer>> held = state.entries();
            NativeEngine.prepareSnapshot(cached);
            state.clear(); state.put(inner[1], 100);
            byte[] newBytes = NativeEngine.writeStateKeyGroupEntries(current, 0);
            NativeEngine.releaseSnapshot(cached);
            count = 0;
            for (Map.Entry<RowData, Integer> e : held) { assertEquals(-20 - e.getKey().getLong(1), e.getValue().longValue()); count++; }
            assertEquals(inner.length, count);
            prior.clear(); NativeEngine.readStateKeyGroupEntries(old, 0, newBytes);
            for (int i = 0; i < inner.length; ++i) assertEquals(-20 - i, prior.get(inner[i]));
            assertEquals(100, state.get(inner[1])); assertNull(state.get(inner[0]));
        } finally { NativeEngine.destroyEngine(cached); NativeEngine.destroyEngine(ordinary); }
    }

    @Test void fixedWidthWindowRowsRestoreOldAndExceptionalPayloadsInBothDirections() throws Exception {
        for (int width : new int[]{8, 16}) {
            RowDataSerializer values = width == 8 ? new RowDataSerializer(new org.apache.flink.table.types.logical.LogicalType[0])
                    : new RowDataSerializer(new BigIntType());
            LongSerializer longs = LongSerializer.INSTANCE;
            byte[] descriptor = TypeAnalyzer.generateStateDescriptor(longs, longs, values);
            byte[] legacy = descriptor.clone();
            assertEquals(width, legacy[legacy.length - 5] & 24);
            legacy[legacy.length - 5] &= ~24;
            long cached = engine(), old = NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
            try {
                long h = NativeEngine.registerState(cached, "fixed", 0, 2, TypeAnalyzer.getTypeId(values), 2, descriptor);
                long prior = NativeEngine.registerState(old, "fixed", 0, 2, TypeAnalyzer.getTypeId(values), 2, legacy);
                int[] lengths = {width, 0, 1, width - 1, width + 1, 24, 251};
                byte[][] payload = new byte[lengths.length][];
                for (int i = 0; i < lengths.length; i++) {
                    payload[i] = new byte[lengths[i]];
                    for (int j = 0; j < lengths[i]; j++) payload[i][j] = (byte) (131 * i + j);
                    NativeEngine.valuePutBlobLongNs(prior, i, 0, 17, 0, payload[i]);
                }
                NativeEngine.prepareSnapshot(old);
                byte[] oldSnapshot = NativeEngine.writeStateKeyGroupEntries(prior, 0);
                NativeEngine.releaseSnapshot(old);
                NativeEngine.readStateKeyGroupEntries(h, 0, oldSnapshot);
                java.nio.ByteBuffer views = NativeEngine.residentControlViews(h).order(java.nio.ByteOrder.nativeOrder());
                for (int round = 0; round < 80; round++) {
                    for (int i = 0; i < lengths.length; i++)
                        assertArrayEquals(payload[i], NativeEngine.valueGetBlobLongNs(h, i, 0, 17, 0));
                }
                boolean compact = false;
                for (int i = 0; i < views.capacity(); i += 128) {
                    long layout = views.getLong(i + 56);
                    if ((layout >>> 56) == (width == 8 ? 247 : 248)) {
                        assertEquals(24 + width, (int) (layout & 65535));
                        compact = true;
                    }
                }
                assertTrue(compact);
                NativeEngine.prepareSnapshot(cached);
                for (int i = 0; i < lengths.length; i++)
                    NativeEngine.valuePutBlobLongNs(h, i, 0, 17, 0, new byte[width + 2]);
                byte[] compactSnapshot = NativeEngine.writeStateKeyGroupEntries(h, 0);
                NativeEngine.releaseSnapshot(cached);
                for (int i = 0; i < lengths.length; i++) NativeEngine.valueClearLongNs(prior, i, 0, 17, 0);
                NativeEngine.readStateKeyGroupEntries(prior, 0, compactSnapshot);
                for (int i = 0; i < lengths.length; i++)
                    assertArrayEquals(payload[i], NativeEngine.valueGetBlobLongNs(prior, i, 0, 17, 0));
            } finally { NativeEngine.destroyEngine(cached); NativeEngine.destroyEngine(old); }
        }
    }

    @Test void residentBigintAccumulatorsPreserveFlinkBytesForEachWindowAndGroup() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        verifyBigintAccumulator(LongSerializer.INSTANCE, -9L);
        verifyBigintAccumulator(new CountingSqlWindowSerializer(),
                new org.apache.flink.table.runtime.operators.window.TimeWindow(-9, 27));
    }

    private <N> void verifyBigintAccumulator(TypeSerializer<N> serializer, N namespace) throws Exception {
        long engine = engine();
        try {
            RowDataSerializer values = new RowDataSerializer(new BigIntType());
            long handle = register(engine, 0, LongSerializer.INSTANCE, serializer, values);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, N, RowData> state = new ForL0ValueState<>(handle, keys,
                    LongSerializer.INSTANCE, serializer, values, null);
            state.setCurrentNamespace(namespace);
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int g = 0; g < 2; g++) {
                keys.currentKeyGroupIndex = g;
                state.update(GenericRowData.of(1L));
                for (int i = 0; i < 160; i++) {
                    BinaryRowData previous = (BinaryRowData) state.value();
                    byte[] held = RowDataKeyAccessor.extractBinaryRowDataBytes(previous);
                    Long number = i % 3 == 0 ? null : Long.valueOf((i & 1) == 0 ? Long.MIN_VALUE + i : Long.MAX_VALUE - i);
                    GenericRowData next = GenericRowData.of((Object) number);
                    next.setRowKind(org.apache.flink.types.RowKind.values()[i % 4]);
                    state.update(next);
                    assertArrayEquals(held, RowDataKeyAccessor.extractBinaryRowDataBytes(previous));
                    assertArrayEquals(RowDataKeyAccessor.extractBinaryRowDataBytes(values.toBinaryRow(next)),
                            RowDataKeyAccessor.extractBinaryRowDataBytes((BinaryRowData) state.value()));
                }
                state.update(GenericRowData.of(111L + g));
            }
            long direct = 0;
            for (int offset = 104; offset < views.capacity(); offset += NamespaceReadIndex.CONTROL_BYTES) {
                direct += views.getLong(offset);
            }
            assertTrue(direct > 150, "resident in-place updates " + direct);
            NativeEngine.prepareSnapshot(engine);
            byte[][] frozen = new byte[2][];
            for (int g = 0; g < 2; g++) {
                keys.currentKeyGroupIndex = g;
                assertEquals(111L + g, state.value().getLong(0));
                state.update(GenericRowData.of(555L + g));
                frozen[g] = NativeEngine.writeStateKeyGroupEntries(handle, g);
                state.clear();
            }
            NativeEngine.releaseSnapshot(engine);
            for (int g = 0; g < 2; g++) {
                NativeEngine.readStateKeyGroupEntries(handle, g, frozen[g]);
                keys.currentKeyGroupIndex = g;
                assertEquals(111L + g, state.value().getLong(0));
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void sqlWindowLongRowsPreserveBytesKindsAndSnapshotOwnership() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (boolean cached : new boolean[]{false, true}) {
            long engine = cached ? engine() : NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer keys = new RowDataSerializer(new BigIntType());
                RowDataSerializer values = new RowDataSerializer(new BigIntType());
                org.apache.flink.table.runtime.operators.window.TimeWindow.Serializer windows =
                        new org.apache.flink.table.runtime.operators.window.TimeWindow.Serializer();
                long handle = NativeEngine.registerState(engine, "test", 0, TypeAnalyzer.getTypeId(keys),
                        TypeAnalyzer.getTypeId(values), TypeAnalyzer.getTypeId(windows),
                        TypeAnalyzer.generateStateDescriptor(keys, windows, values));
                ForL0KeyContext<RowData> context = context(GenericRowData.of(1L));
                ForL0ValueState<RowData, org.apache.flink.table.runtime.operators.window.TimeWindow, RowData> state =
                        new ForL0ValueState<>(handle, context, keys, windows, values, null);
                org.apache.flink.table.runtime.operators.window.TimeWindow window =
                        new org.apache.flink.table.runtime.operators.window.TimeWindow(-10, 20);
                state.setCurrentNamespace(window);
                org.apache.flink.core.memory.DataOutputSerializer keyBytes = new org.apache.flink.core.memory.DataOutputSerializer(32);
                windows.serialize(window, keyBytes); keys.serialize(context.currentKey, keyBytes);
                for (org.apache.flink.types.RowKind kind : org.apache.flink.types.RowKind.values()) {
                    for (Long number : Arrays.asList(null, Long.MIN_VALUE, 0L, Long.MAX_VALUE)) {
                        GenericRowData input = GenericRowData.of((Object) number); input.setRowKind(kind);
                        state.update(input);
                        byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytes(values.toBinaryRow(input));
                        org.apache.flink.core.memory.DataOutputSerializer expected = new org.apache.flink.core.memory.DataOutputSerializer(64);
                        expected.writeInt(1); expected.writeInt(keyBytes.length()); expected.write(keyBytes.getCopyOfBuffer());
                        expected.writeInt(raw.length); expected.write(raw);
                        NativeEngine.prepareSnapshot(engine);
                        assertArrayEquals(expected.getCopyOfBuffer(), NativeEngine.writeStateKeyGroupEntries(handle, 0));
                        byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
                        state.update(GenericRowData.of(99L));
                        NativeEngine.releaseSnapshot(engine);
                        NativeEngine.readKeyGroupData(engine, 0, snapshot);
                        RowData restored = state.value();
                        assertEquals(kind, restored.getRowKind()); assertEquals(number == null, restored.isNullAt(0));
                        if (number != null) assertEquals(number.longValue(), restored.getLong(0));
                        state.update(restored);
                        assertEquals(kind, state.value().getRowKind());
                    }
                }
                if (cached) {
                    context.currentKey = GenericRowData.of(7L);
                    state.update(GenericRowData.of(1L));
                    state.update(GenericRowData.of(2L));
                    assertEquals(2L, state.value().getLong(0));
                    assertNotNull(NativeEngine.stateCacheMemoryRange(handle));
                }
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void sqlWindowTableRestoresWithHotCacheOnAndOff() throws Exception {
        long current = engine();
        long previous = NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer keys = new RowDataSerializer(new BigIntType());
            RowDataSerializer values = rowSerializer();
            CountingSqlWindowSerializer windows = new CountingSqlWindowSerializer();
            long oldHandle = register(previous, 0, keys, windows, values);
            long newHandle = register(current, 0, keys, windows, values);
            assertNotNull(NativeEngine.stateCacheMemoryRange(newHandle));

            GenericRowData nullKey = GenericRowData.of((Object) null);
            nullKey.setRowKind(org.apache.flink.types.RowKind.UPDATE_BEFORE);
            ForL0KeyContext<RowData> ctx = context(nullKey);
            ForL0ValueState<RowData, org.apache.flink.table.runtime.operators.window.TimeWindow, RowData> oldState =
                    new ForL0ValueState<>(oldHandle, ctx, keys, windows, values, null);
            ForL0ValueState<RowData, org.apache.flink.table.runtime.operators.window.TimeWindow, RowData> newState =
                    new ForL0ValueState<>(newHandle, ctx, keys, windows, values, null);
            org.apache.flink.table.runtime.operators.window.TimeWindow window =
                    new org.apache.flink.table.runtime.operators.window.TimeWindow(-19, 7);
            oldState.setCurrentNamespace(window);
            newState.setCurrentNamespace(window);
            oldState.update(row(1, "from the previous layout"));
            NativeEngine.prepareSnapshot(previous);
            byte[] legacyBytes = NativeEngine.writeStateKeyGroupEntries(oldHandle, 0);
            NativeEngine.releaseSnapshot(previous);
            NativeEngine.readStateKeyGroupEntries(newHandle, 0, legacyBytes);
            assertEquals("from the previous layout", newState.value().getString(1).toString());
            newState.update(newState.value()); // A borrowed address can be the input to a promotion.
            assertEquals("from the previous layout", newState.value().getString(1).toString());
            assertArrayEquals(legacyBytes, NativeEngine.writeStateKeyGroupEntries(newHandle, 0));
            traffic(newHandle);

            NativeEngine.prepareSnapshot(current);
            newState.update(row(2, "after checkpoint"));
            byte[] beforeUpdate = NativeEngine.writeStateKeyGroupEntries(newHandle, 0);
            NativeEngine.releaseSnapshot(current);
            assertArrayEquals(legacyBytes, beforeUpdate);
            byte[] newBytes = NativeEngine.writeStateKeyGroupEntries(newHandle, 0);
            NativeEngine.readStateKeyGroupEntries(oldHandle, 0, newBytes);
            assertEquals("after checkpoint", oldState.value().getString(1).toString());
            assertEquals(1L, NativeEngine.stateEntries(newHandle));

            org.apache.flink.core.memory.DataInputDeserializer found =
                    new org.apache.flink.core.memory.DataInputDeserializer(
                            NativeEngine.getWindowStateKeys(newHandle, true, 0, 0));
            assertEquals(1, found.readInt());
            RowData restoredKey = keys.deserialize(found);
            assertTrue(restoredKey.isNullAt(0));
            assertEquals(nullKey.getRowKind(), restoredKey.getRowKind());
            assertEquals(window, windows.deserialize(found));
            newState.clear();
            assertNull(newState.value());
        } finally {
            NativeEngine.destroyEngine(current);
            NativeEngine.destroyEngine(previous);
        }
    }

    @Test void serializedKeysShareWholeWindowTablesAndKeepCheckpointEncoding() throws Exception {
        checkGenericWindowKeys(LongSerializer.INSTANCE, -42L, -41L);
        CountingSqlWindowSerializer windows = new CountingSqlWindowSerializer();
        checkGenericWindowKeys(windows,
                new org.apache.flink.table.runtime.operators.window.TimeWindow(-9, 0),
                new org.apache.flink.table.runtime.operators.window.TimeWindow(-8, 1));
    }

    private <N> void checkGenericWindowKeys(TypeSerializer<N> windows, N first, N second) throws Exception {
        long current = engine();
        long previous = NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
        try {
            TypeSerializer<String> keys = StringSerializer.INSTANCE;
            RowDataSerializer values = rowSerializer();
            long oldHandle = register(previous, 0, keys, windows, values);
            long newHandle = register(current, 0, keys, windows, values);
            final String key = "A serialized key that exceeds the inline slot";
            ForL0KeyContext<String> ctx = context(key);
            ForL0ValueState<String, N, RowData> oldState =
                    new ForL0ValueState<>(oldHandle, ctx, keys, windows, values, null);
            ForL0ValueState<String, N, RowData> newState =
                    new ForL0ValueState<>(newHandle, ctx, keys, windows, values, null);
            oldState.setCurrentNamespace(first);
            oldState.update(row(8, "restored"));
            byte[] legacy = NativeEngine.writeStateKeyGroupEntries(oldHandle, 0);
            NativeEngine.readStateKeyGroupEntries(newHandle, 0, legacy);
            newState.setCurrentNamespace(first);
            assertEquals("restored", newState.value().getString(1).toString());
            newState.update(row(8, "restored"));
            assertEquals("restored", newState.value().getString(1).toString());
            traffic(newHandle);
            assertArrayEquals(legacy, NativeEngine.writeStateKeyGroupEntries(newHandle, 0));

            newState.setCurrentNamespace(second);
            assertNull(newState.value());
            newState.update(row(9, "separate window"));
            NativeEngine.readStateKeyGroupEntries(oldHandle, 0, NativeEngine.writeStateKeyGroupEntries(newHandle, 0));
            oldState.setCurrentNamespace(second);
            assertEquals("separate window", oldState.value().getString(1).toString());
            org.apache.flink.core.memory.DataInputDeserializer cursor =
                    new org.apache.flink.core.memory.DataInputDeserializer(
                            NativeEngine.getWindowStateKeys(newHandle, false,
                                    first instanceof Long ? (Long) first : TypeAnalyzer.windowStart(first),
                                    first instanceof Long ? 0 : TypeAnalyzer.windowEnd(first)));
            assertEquals(1, cursor.readInt());
            assertEquals(key, keys.deserialize(cursor));
            newState.clear();
            assertNull(newState.value());
            newState.setCurrentNamespace(first);
            assertEquals(8L, newState.value().getLong(0));
        } finally {
            NativeEngine.destroyEngine(current);
            NativeEngine.destroyEngine(previous);
        }
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
            assertNull(NativeEngine.stateCacheMemoryRange(handle));
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

    @Test void fixedKeyTablesPreserveNullsAndRowKindsAcrossCheckpoints() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (int arity : new int[]{2, 3, 8}) {
            org.apache.flink.table.types.logical.LogicalType[] types =
                    new org.apache.flink.table.types.logical.LogicalType[arity];
            Arrays.fill(types, new BigIntType());
            RowDataSerializer rows = new RowDataSerializer(types);
            for (boolean enabled : new boolean[]{false, true}) {
                verifyFixedKeyCheckpoint(rows, arity, enabled, LongSerializer.INSTANCE, Arrays.asList(-7L, 0L, 99L, 35L));
                verifyFixedKeyCheckpoint(rows, arity, enabled, IntSerializer.INSTANCE, Arrays.asList(-7, 0, 99, 35));
                verifyFixedKeyCheckpoint(rows, arity, enabled, DoubleSerializer.INSTANCE, Arrays.asList(-7.25, -0.0, 99.0, 35.5));
                verifyFixedKeyCheckpoint(rows, arity, enabled, StringSerializer.INSTANCE, Arrays.asList("null", "", "zero", "kind"));
            }
        }
    }

    private <V> void verifyFixedKeyCheckpoint(RowDataSerializer rows, int arity, boolean enabled,
                                               TypeSerializer<V> values, List<V> expected) throws Exception {
        long engine = NativeEngine.createEngine(0, 2, 2, enabled, 128 * 1024, true, 65536, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, rows, VoidNamespaceSerializer.INSTANCE, values);
            List<RowData> inputs = new java.util.ArrayList<>();
            for (int i = 0; i < expected.size(); i++) {
                GenericRowData row = new GenericRowData(arity);
                for (int f = 0; f < arity; f++) row.setField(f, (long) f);
                if (i == 0) row.setField(0, null);
                if (i == 1) row.setField(arity - 1, null);
                if (i == 3) row.setRowKind(org.apache.flink.types.RowKind.DELETE);
                inputs.add(row);
            }
            ForL0KeyContext<RowData> keys = context(inputs.get(0));
            ForL0ValueState<RowData, VoidNamespace, V> state = new ForL0ValueState<>(
                    handle, keys, rows, VoidNamespaceSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            for (int i = 0; i < inputs.size(); i++) {
                keys.setCurrentKey(inputs.get(i));
                assertNull(state.value());
                state.update(expected.get(i));
            }
            assertEquals(inputs.size(), NativeEngine.stateEntries(handle));
            NativeEngine.prepareSnapshot(engine);
            keys.setCurrentKey(inputs.get(0));
            state.clear();
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            for (RowData input : inputs) {
                keys.setCurrentKey(rows.toBinaryRow(input).copy());
                state.clear();
            }
            assertEquals(0, NativeEngine.stateEntries(handle));
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            for (int i = 0; i < inputs.size(); i++) {
                keys.setCurrentKey(rows.toBinaryRow(inputs.get(i)).copy());
                assertEquals(expected.get(i), state.value());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void aFullResidentProbeGroupContinuesAcrossTheEndOfTheNamespaceTable() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = engine();
        try {
            LongSerializer longs = LongSerializer.INSTANCE;
            long handle = register(engine, 0, longs, VoidNamespaceSerializer.INSTANCE, longs);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, VoidNamespace, Long> state = new ForL0ValueState<>(handle, keys,
                    longs, VoidNamespaceSerializer.INSTANCE, longs, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            long[] collisions = new long[25];
            for (long key = 0, count = 0; count < collisions.length; key++) {
                long h = (key ^ (key >>> 30)) * 0xbf58476d1ce4e5b9L;
                h = (h ^ (h >>> 27)) * 0x94d049bb133111ebL;
                if ((((h ^ (h >>> 31)) >>> 7) & 1023) == 1019) collisions[(int) count++] = key;
            }
            for (int i = 0; i < 24; i++) { keys.currentKey = collisions[i]; state.update(17L + i); }
            for (int i = 0; i < 200; i++) { keys.currentKey = collisions[i % 24]; assertEquals(Long.valueOf(17L + i % 24), state.value()); }
            ByteBuffer cells = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            assertTrue(cells.getLong(48) != 0 && cells.getLong(8) >= 31 && cells.getLong(8) <= 1023);
            NamespaceReadIndex direct = NamespaceReadIndex.forState(handle, 0, true);
            for (int i = 0; i < 24; i++) {
                cells.putLong(120, 0); // exercise the probing sequence on every lookup
                assertEquals(NamespaceReadIndex.NUMBER, direct.readNumber(0, collisions[i]));
                assertEquals(17L + i, direct.result);
            }
            assertEquals(NamespaceReadIndex.ABSENT, direct.readNumber(0, collisions[24]));
            keys.currentKey = collisions[24]; state.update(81L);
            assertEquals(Long.valueOf(81L), state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void republishingAWindowBucketReadsOnlyTheNewWholeTable() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = engine();
        try {
            LongSerializer longs = LongSerializer.INSTANCE;
            RowDataSerializer rows = new RowDataSerializer(new BigIntType());
            long handle = register(engine, 0, longs, longs, rows);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, keys, longs, longs, rows, null);
            ByteBuffer cells = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            // 0 and 5 select the same one of four namespace descriptor cells.
            for (long ns : new long[]{0, 5}) {
                state.setCurrentNamespace(ns);
                for (long i = 0; i < 40; i++) { keys.currentKey = i; state.update(GenericRowData.of(100 * ns + i)); }
            }
            for (int wave = 0; wave < 8; wave++) {
                long ns = (wave & 1) == 0 ? 0 : 5;
                state.setCurrentNamespace(ns); keys.currentKey = 7L;
                for (int i = 0; i < 100; i++) assertEquals(100 * ns + 7, state.value().getLong(0));
                assertEquals(ns, cells.getLong(24));
                assertTrue(cells.getLong(8) > 3 && (int) cells.getLong(120) > 0);
            }
            state.setCurrentNamespace(5L); state.clear(); assertNull(state.value());
            state.setCurrentNamespace(0L); assertEquals(7L, state.value().getLong(0));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentReadPositionChecksTheEntireKeyAcrossEraseSnapshotAndGrowth() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 2 * 1024 * 1024, true, 512 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, VoidNamespace, Long> state = new ForL0ValueState<>(handle, keys,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            long a = 77L, b = (1L << 32) | 76L; // same 32-bit fingerprint
            for (int i = 0; i < 40; i++) { keys.currentKey = (long) i; state.update((long) i); }
            keys.currentKey = a; state.update(179L);
            keys.currentKey = b; state.update(281L);
            for (int i = 0; i < 260; i++) {
                keys.currentKey = (i & 1) == 0 ? a : b;
                assertEquals(Long.valueOf((i & 1) == 0 ? 179L : 281L), state.value());
            }
            ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            assertTrue(view.getLong(48) != 0 && (int) view.getLong(120) > 0);
            NativeEngine.prepareSnapshot(engine);
            keys.currentKey = b; state.clear();
            keys.currentKey = a; state.update(811L);
            keys.currentKey = b; assertNull(state.value());
            keys.currentKey = a; assertEquals(Long.valueOf(811L), state.value());
            NativeEngine.releaseSnapshot(engine);
            for (int i = 100; i < 6000; i++) { keys.currentKey = (long) i; state.update((long) i); }
            keys.currentKey = a; assertEquals(Long.valueOf(811L), state.value());
            keys.currentKey = b; assertNull(state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void repeatedMissingRowsStayScopedToTheirWholeNamespaceUntilInserted() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 128 * 1024, true, 16 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, VoidNamespace, Long> state = new ForL0ValueState<>(handle, keys,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            ByteBuffer cells = NativeEngine.rootMembershipViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int i = 0; i < 4096; i++) {
                keys.currentKeyGroupIndex = i & 1;
                keys.currentKey = (long) i;
                assertNull(state.value());
                state.update((long) i);
            }
            final long valid = 1L << 62;
            long[] missing = new long[2];
            for (int group = 0; group < 2; group++) {
                keys.currentKeyGroupIndex = group;
                for (int i = 0; i < 4096; i++) {
                    keys.currentKey = -1L - i - group * 4096L;
                    assertNull(state.value());
                    if ((cells.getLong(32 * group + 8) & valid) != 0) break;
                }
                assertTrue((cells.getLong(32 * group + 8) & valid) != 0);
                missing[group] = keys.currentKey;
            }
            for (int group = 0; group < 2; group++) {
                keys.currentKeyGroupIndex = group;
                keys.currentKey = missing[group];
                long before = cells.getLong(32 * group + 24);
                for (int i = 0; i < 128; i++) assertNull(state.value());
                assertTrue(cells.getLong(32 * group + 24) - before >= 120);
                state.update(100L + group);
                assertEquals(0L, cells.getLong(32 * group + 8) & valid);
                assertEquals(Long.valueOf(100L + group), state.value());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void retiringPositiveRootReenablesItsOrdinaryNegativeIndex() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 128 * 1024, true, 16 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0ValueState<Long, VoidNamespace, Long> state = new ForL0ValueState<>(handle, keys,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            ByteBuffer membership = NativeEngine.rootMembershipViews(handle).order(java.nio.ByteOrder.nativeOrder());
            state.update(7L);
            for (int i = 0; i < 128; i++) assertEquals(7L, state.value());
            assertTrue(controls.getLong(48) != 0);
            int firstOrdinary = 1;
            while (firstOrdinary < 32768 && controls.getLong(48) != 0) {
                keys.currentKey = (long) firstOrdinary;
                state.update(7L + firstOrdinary++);
            }
            assertEquals(0, controls.getLong(0));
            assertEquals(0, controls.getLong(48));
            assertEquals(0, controls.getLong(56));
            long skips = membership.getLong(24);
            for (int i = firstOrdinary; i < 16384; i++) {
                keys.currentKey = (long) i;
                assertNull(state.value());
                state.update(7L + i);
            }
            assertTrue(membership.getLong(24) > skips + 50);
            for (int i = 0; i < 16384; i += 31) { keys.currentKey = (long)i; assertEquals(7L + i, state.value()); }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void sharedNumberIndexMatchesSignedNativeKeysAcrossGroups() throws Exception {
        for (boolean integers : new boolean[]{false, true}) {
            if (integers) checkSharedNumbers(org.apache.flink.api.common.typeutils.base.IntSerializer.INSTANCE,
                    i -> Integer.MIN_VALUE + i * 73);
            else checkSharedNumbers(LongSerializer.INSTANCE, i -> Long.MIN_VALUE + i * 100000000003L);
        }
    }

    private <K> void checkSharedNumbers(TypeSerializer<K> serializer, java.util.function.IntFunction<K> toKey)
            throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        // Outgrow both initially resident roots so the shared negative index is exercised.
        final int keyCount = 16 * 1024;
        long engine = NativeEngine.createEngine(0, 2, 2, true, 128 * 1024,
                true, 16 * 1024, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, serializer, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<K> keys = context(toKey.apply(0));
            ForL0ValueState<K, VoidNamespace, Long> state = new ForL0ValueState<>(
                    handle, keys, serializer, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            for (int i = 0; i < keyCount; ++i) {
                keys.setCurrentKeyGroupIndex(i & 1); keys.setCurrentKey(toKey.apply(i));
                assertNull(state.value());
                state.update((long) i);
            }
            java.nio.ByteBuffer cells = NativeEngine.rootMembershipViews(handle).order(java.nio.ByteOrder.nativeOrder());
            assertTrue(cells.getLong(24) + cells.getLong(56) > 50);
            for (int i = keyCount - 1; i >= 0; --i) {
                keys.setCurrentKeyGroupIndex(i & 1); keys.setCurrentKey(toKey.apply(i));
                assertEquals(Long.valueOf(i), state.value());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void sharedRootIndexNeverHidesPublishedOrRestoredRows() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        // Exceed the entire pool so that this test necessarily exercises the DRAM root index.
        final int keyCount = 16 * 1024;
        long engine = NativeEngine.createEngine(0, 2, 2, true, 128 * 1024, true, 16 * 1024, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer pairs = keySerializer();
            long handle = register(engine, 0, pairs, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<RowData> keys = context(GenericRowData.of(0L, 0L));
            ForL0ValueState<RowData, VoidNamespace, Long> state = new ForL0ValueState<>(
                    handle, keys, pairs, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            java.nio.ByteBuffer cells = NativeEngine.rootMembershipViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int i = 0; i < keyCount; ++i) {
                keys.setCurrentKeyGroupIndex(i & 1);
                keys.setCurrentKey(GenericRowData.of(Long.MIN_VALUE + i, -17L * i));
                assertNull(state.value());
                state.update(10L + i);
            }
            assertTrue(cells.getLong(24) + cells.getLong(56) > 100);
            for (int i = 0; i < keyCount; ++i) {
                keys.setCurrentKeyGroupIndex(i & 1);
                keys.setCurrentKey(GenericRowData.of(Long.MIN_VALUE + i, -17L * i));
                assertEquals(Long.valueOf(10L + i), state.value());
            }
            NativeEngine.prepareSnapshot(engine);
            keys.setCurrentKeyGroupIndex(0);
            keys.setCurrentKey(GenericRowData.of(Long.MIN_VALUE + 82, -17L * 82));
            state.clear();
            assertNull(state.value());
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            assertEquals(Long.valueOf(92L), state.value());
            long[] counters = new long[7];
            NativeEngine.getHotCacheStats(handle, counters);
            assertTrue(counters[1] >= 2L * keyCount);
            long previous = counters[1];
            NativeEngine.getHotCacheStats(handle, counters);
            assertEquals(previous, counters[1]);
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
                                                        TypeSerializer<V> values, V value,
                                                        boolean cached) throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = cached ? engine() : NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
        try {
            long handle = register(engine, 0, keys, LongSerializer.INSTANCE, values);
            ForL0ValueState<K, Long, V> state = new ForL0ValueState<>(handle, context(key),
                    keys, LongSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(123L);
            state.update(value);
            if (cached) {
                state.update(value);
                assertNotNull(state.value());
            }
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
            if (cached) traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericLongNamespacesKeepLegacyCheckpointEncoding() throws Exception {
        for (boolean cached : new boolean[]{false, true}) {
            checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, LongSerializer.INSTANCE, Long.MIN_VALUE, cached);
            checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, IntSerializer.INSTANCE, -17, cached);
            checkLegacyLongNamespaceEncoding(LongSerializer.INSTANCE, -42L, DoubleSerializer.INSTANCE, -0.25d, cached);
        }
    }

    @Test void rowLongNamespacesPreserveNullsRowKindAndLegacyCheckpointEncoding() throws Exception {
        RowDataSerializer keys = new RowDataSerializer(new BigIntType());
        RowDataSerializer values = new RowDataSerializer(new BigIntType());
        GenericRowData nullable = GenericRowData.of((Object) null);
        nullable.setRowKind(org.apache.flink.types.RowKind.UPDATE_AFTER);
        GenericRowData nullKey = GenericRowData.of((Object) null);
        nullKey.setRowKind(org.apache.flink.types.RowKind.UPDATE_BEFORE);
        for (boolean cached : new boolean[]{false, true}) {
            checkLegacyLongNamespaceEncoding(keys, GenericRowData.of(-42L), values, nullable, cached);
            checkLegacyLongNamespaceEncoding(keys, nullKey, values, GenericRowData.of(9L), cached);
            checkLegacyLongNamespaceEncoding(keys, GenericRowData.of(-42L), rowSerializer(), row(7L, "payload"), cached);
        }
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

    @Test void fixedWidthMapEntriesRetainSerializerBytesAcrossCheckpoints() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        TypeSerializer<org.apache.flink.api.java.tuple.Tuple2<Integer, Long>> pairs =
                org.apache.flink.api.common.typeinfo.TypeInformation.of(
                        new org.apache.flink.api.common.typeinfo.TypeHint<org.apache.flink.api.java.tuple.Tuple2<Integer, Long>>() {})
                        .createSerializer(new org.apache.flink.api.common.ExecutionConfig());
        assertEquals(12, pairs.getLength());
        for (boolean enabled : new boolean[]{false, true}) {
            verifyInlineMap(enabled, IntSerializer.INSTANCE, i -> -i);
            verifyInlineMap(enabled, pairs, i -> org.apache.flink.api.java.tuple.Tuple2.of(i, -123456789012L - i));
        }
    }

    private <V> void verifyInlineMap(boolean enabled, TypeSerializer<V> values, java.util.function.IntFunction<V> value)
            throws Exception {
        long engine = NativeEngine.createEngine(0, 2, 2, enabled, 128 * 1024, true, 65536, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer keys = rowSerializer();
            MapSerializer<RowData, V> maps = new MapSerializer<>(keys, values);
            byte[] declared = TypeAnalyzer.generateStateDescriptor(LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            byte[] variable = declared.clone();
            // A primitive int also has a self-describing native type without the explicit width.
            assertEquals(values.getLength(), java.nio.ByteBuffer.wrap(declared).getInt(declared.length - 4));
            if (values == IntSerializer.INSTANCE) Arrays.fill(variable, variable.length - 4, variable.length, (byte) -1);
            List<ForL0MapState<Long, VoidNamespace, RowData, V>> states = new java.util.ArrayList<>();
            long[] handles = new long[2];
            for (int i = 0; i < handles.length; i++) {
                handles[i] = NativeEngine.registerState(engine, "map-" + i, 2, TypeAnalyzer.TYPE_INT64,
                        TypeAnalyzer.TYPE_MAP, TypeAnalyzer.TYPE_VOID_NS, i == 0 ? declared : variable);
                ForL0MapState<Long, VoidNamespace, RowData, V> state = new ForL0MapState<>(handles[i], context(3L),
                        LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, keys, values);
                state.setCurrentNamespace(VoidNamespace.INSTANCE);
                states.add(state);
            }
            Map<RowData, V> expected = new HashMap<>();
            for (int i = 0; i < 21; i++) expected.put(keys.toBinaryRow(row(i, "key" + i)).copy(), value.apply(i));
            for (int source = 0; source < 2; source++) {
                ForL0MapState<Long, VoidNamespace, RowData, V> state = states.get(source);
                state.clear();
                for (Map.Entry<RowData, V> entry : expected.entrySet()) state.put(entry.getKey(), entry.getValue());
                NativeEngine.prepareSnapshot(engine);
                RowData first = expected.keySet().iterator().next();
                state.remove(first);
                assertFalse(state.contains(first));
                byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handles[source], 0);
                NativeEngine.releaseSnapshot(engine);
                org.apache.flink.core.memory.DataInputDeserializer wire = new org.apache.flink.core.memory.DataInputDeserializer(frozen);
                assertEquals(1, wire.readInt());
                assertEquals(0, wire.readByte());
                assertEquals(3L, wire.readLong());
                assertEquals(wire.available() - 4, wire.readInt());
                assertEquals(expected.size(), wire.readInt());
                Map<RowData, V> decoded = new HashMap<>();
                for (int i = 0; i < expected.size(); i++) decoded.put(keys.copy(keys.deserialize(wire)), values.deserialize(wire));
                assertEquals(0, wire.available());
                assertEquals(expected, decoded);
                // Also restore independently produced bytes, including the canonical four-byte value prefix.
                org.apache.flink.core.memory.DataOutputSerializer payload = new org.apache.flink.core.memory.DataOutputSerializer(128);
                payload.writeInt(expected.size());
                for (Map.Entry<RowData, V> row : expected.entrySet()) {
                    keys.serialize(row.getKey(), payload);
                    values.serialize(row.getValue(), payload);
                }
                org.apache.flink.core.memory.DataOutputSerializer independent = new org.apache.flink.core.memory.DataOutputSerializer(128);
                independent.writeInt(1); independent.writeByte(0); independent.writeLong(3L);
                independent.writeInt(payload.length()); independent.write(payload.getCopyOfBuffer());
                ForL0MapState<Long, VoidNamespace, RowData, V> restored = states.get(1 - source);
                restored.clear();
                NativeEngine.readStateKeyGroupEntries(handles[1 - source], 0, source == 0 ? frozen : independent.getCopyOfBuffer());
                Map<RowData, V> actual = new HashMap<>();
                for (Map.Entry<RowData, V> entry : restored.entries()) actual.put(entry.getKey(), entry.getValue());
                assertEquals(expected, actual);
                for (Map.Entry<RowData, V> entry : expected.entrySet()) {
                    assertTrue(restored.contains(entry.getKey()));
                    assertEquals(entry.getValue(), restored.get(entry.getKey()));
                }
                long iterator = NativeEngine.mapIteratorCreate(handles[1 - source], 3L, 0);
                assertNotEquals(0, iterator);
                try {
                    restored.clear();
                    actual.clear();
                    byte[] bytes;
                    while ((bytes = NativeEngine.mapIteratorNext(iterator)) != null) {
                        org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(bytes);
                        actual.put(keys.copy(keys.deserialize(in)), values.deserialize(in));
                        assertEquals(0, in.available());
                    }
                    assertEquals(expected, actual);
                } finally { NativeEngine.mapIteratorDestroy(iterator); }
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericRowMapIterationPreservesBinaryKeyEquality() throws Exception {
        for (boolean cached : new boolean[]{false, true}) {
            long engine = NativeEngine.createEngine(0, 2, 2, cached, 8 * 1024 * 1024,
                    true, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer keys = new RowDataSerializer(new BigIntType());
                ListSerializer<RowData> lists = new ListSerializer<>(keys);
                MapSerializer<RowData, List<RowData>> maps = new MapSerializer<>(keys, lists);
                long handle = NativeEngine.registerState(engine, "prices", 2, TypeAnalyzer.TYPE_INT64,
                        TypeAnalyzer.getTypeId(maps), TypeAnalyzer.TYPE_VOID_NS,
                        TypeAnalyzer.generateStateDescriptor(LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps));
                ForL0MapState<Long, VoidNamespace, RowData, List<RowData>> state = new ForL0MapState<>(handle,
                        context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, keys, lists);
                state.setCurrentNamespace(VoidNamespace.INSTANCE);
                RowData first = keys.toBinaryRow(GenericRowData.of(7L)).copy();
                RowData second = keys.toBinaryRow(GenericRowData.of(9L)).copy();
                state.put(first, Arrays.asList(GenericRowData.of(42L)));
                state.put(second, Arrays.asList(GenericRowData.of(43L)));
                Map<Long, RowData> restored = new HashMap<>();
                for (Map.Entry<RowData, List<RowData>> entry : state.entries()) {
                    restored.put(entry.getKey().getLong(0), entry.getKey());
                }
                assertEquals(first, restored.get(7L));
                assertEquals(second, restored.get(9L));
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void packedListsReadAndWriteFlinkSerializerBytesAcrossMoves() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        ListSerializer<String> serializer = new ListSerializer<>(StringSerializer.INSTANCE);
        List<String> expected = Arrays.asList("", "北京", String.join("", java.util.Collections.nCopies(180, "ab")));
        org.apache.flink.core.memory.DataOutputSerializer payload = new org.apache.flink.core.memory.DataOutputSerializer(256);
        serializer.serialize(expected, payload);
        org.apache.flink.core.memory.DataOutputSerializer wire = new org.apache.flink.core.memory.DataOutputSerializer(256);
        wire.writeInt(1);
        wire.writeByte(0);
        wire.writeLong(3L);
        wire.writeInt(payload.length());
        wire.write(payload.getCopyOfBuffer());
        byte[] independent = wire.getCopyOfBuffer();
        for (boolean cached : new boolean[]{false, true}) {
            long engine = NativeEngine.createEngine(0, 2, 2, cached, 128 * 1024, true, 65536, 0, 16, 0, .875, 0);
            try {
                long handle = register(engine, 1, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
                ForL0ListState<Long, VoidNamespace, String> state = new ForL0ListState<>(handle, context(3L),
                        LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, StringSerializer.INSTANCE, serializer, null);
                state.setCurrentNamespace(VoidNamespace.INSTANCE);
                NativeEngine.readStateKeyGroupEntries(handle, 0, independent);
                List<String> retained = state.getInternal();
                assertEquals(expected, retained);
                NativeEngine.prepareSnapshot(engine);
                state.add("tail");
                byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.releaseSnapshot(engine);
                assertArrayEquals(independent, frozen);
                assertEquals(expected, retained);
                assertEquals(4, state.getInternal().size());
                state.clear();
                state.addAll(expected);
                assertArrayEquals(payload.getCopyOfBuffer(), NativeEngine.listGet(handle, 3L, 0));
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void oversizedRootUsesExactProbeControlsWithoutReturningDramAsPoolRows() throws Exception {
        long engine = engine();
        try {
            LongSerializer keys = LongSerializer.INSTANCE;
            RowDataSerializer rows = rowSerializer();
            long handle = register(engine, 0, keys, VoidNamespaceSerializer.INSTANCE, rows);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0ValueState<Long, org.apache.flink.runtime.state.VoidNamespace, RowData> state = new ForL0ValueState<>(
                    handle, ctx, keys, VoidNamespaceSerializer.INSTANCE, rows, null);
            java.nio.ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            String payload = "slice-".repeat(80);
            for (long i = 0; i < 8192; i++) { ctx.currentKey = i; state.update(row(i, payload)); }
            for (int i = 0; i < 8192; i++) { ctx.currentKey = (long) (i & 1023); state.update(row(ctx.currentKey, payload)); }
            for (int i = 0; i < 2000; i++) {
                ctx.currentKey = -1L - i; assertNull(state.value());
                ctx.currentKey = (long) i; RowData found = state.value();
                assertEquals(i, found.getLong(0)); assertEquals(payload, found.getString(1).toString());
            }
            assertNotEquals(0L, controls.getLong(0));
            assertEquals(0L, controls.getLong(48));
            assertTrue(controls.getLong(40) > 1000);
            NamespaceReadIndex probes = NamespaceReadIndex.forState(handle, 0, true);
            int handedOff = 0;
            for (long key = 0; key < 48; key++) {
                controls.putLong(120, 0L);
                assertEquals(NamespaceReadIndex.UNKNOWN, probes.readNumber(0, key));
                long position = controls.getLong(120);
                if (position != 0) {
                    assertEquals(key, position >>> 32);
                    handedOff++;
                }
                byte[] found = NativeEngine.valueGetLongString(handle, key, 0);
                assertEquals(key, RowDataKeyAccessor.wrapBinaryRowData(found, 2).getLong(0));
            }
            assertTrue(handedOff > 20);
            // A collision or stale ordinal is only a hint, even with the right fingerprint.
            controls.putLong(120, (999L << 32) | 1);
            byte[] found = NativeEngine.valueGetLongString(handle, 999L, 0);
            assertEquals(999L, RowDataKeyAccessor.wrapBinaryRowData(found, 2).getLong(0));
            controls.putLong(120, (99999L << 32) | 1);
            assertNull(NativeEngine.valueGetLongString(handle, 99999L, 0));
            NativeEngine.prepareSnapshot(engine);
            for (int i = 0; i < 128; i++) { ctx.currentKey = (long) i; state.clear(); assertNull(state.value()); }
            byte[] checkpoint = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, checkpoint);
            for (int i = 0; i < 128; i++) { ctx.currentKey = (long) i; assertEquals(i, state.value().getLong(0)); }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentSlotsReadInlineAndIndirectRowsWithoutLosingFlinkKeyIdentity() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = new RowDataSerializer(new BigIntType());
            RowDataSerializer rows = rowSerializer();
            LongSerializer namespaces = LongSerializer.INSTANCE;
            long handle = register(engine, 0, keys, namespaces, rows);
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of(0L));
            ForL0ValueState<RowData, Long, RowData> state = new ForL0ValueState<>(handle, ctx, keys, namespaces, rows, null);
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            String extended = "右側".repeat(40);
            GenericRowData zero = GenericRowData.of(0L);
            GenericRowData nullable = GenericRowData.of((Object) null);
            GenericRowData deleted = GenericRowData.ofKind(org.apache.flink.types.RowKind.DELETE, 0L);
            GenericRowData[] inputs = {zero, nullable, deleted};
            String[] texts = {"x", extended, null};
            state.setCurrentNamespace(-43L);
            for (int n = 0; n < inputs.length; n++) { ctx.currentKey = inputs[n]; state.update(row(n, texts[n])); }
            for (int i = 0; i < 400; i++) {
                int n = i % inputs.length;
                ctx.currentKey = inputs[n];
                RowData actual = state.value();
                assertEquals(n, actual.getLong(0));
                if (texts[n] == null) assertTrue(actual.isNullAt(1));
                else assertEquals(texts[n], actual.getString(1).toString());
            }
            assertTrue(windowControlCount(views, 64) > 300);
            ctx.currentKey = zero;
            RowData first = state.value();
            ctx.currentKey = nullable;
            assertEquals(extended, state.value().getString(1).toString());
            assertEquals("x", first.getString(1).toString());
            NativeEngine.prepareSnapshot(engine);
            state.update(row(99, "updated"));
            for (long i = 1; i < 1800; i++) { ctx.currentKey = GenericRowData.of(i); state.update(row(i, extended)); }
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            ctx.currentKey = nullable;
            assertEquals(extended, state.value().getString(1).toString());
            ctx.currentKey = zero;
            assertEquals("x", state.value().getString(1).toString());
            ctx.currentKey = deleted;
            assertEquals(2L, state.value().getLong(0));
            state.setCurrentNamespace(43L);
            assertNull(state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void alternatingActiveWindowsHitTheirOwnWholeTablesAndFreezeTogether() throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> ctx = context(11L);
            ForL0ValueState<Long, Long, Long> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            long[] windows = {0, 1, 3, 4};
            for (long ns : windows) {
                state.setCurrentNamespace(ns);
                state.update(ns + 11);
                for (int i = 0; i < 8; i++) assertEquals(ns + 11, state.value());
            }
            long lastHits = windowControlCount(views, 64);
            for (int i = 0; i < 256; i++) {
                long ns = windows[i & 3];
                state.setCurrentNamespace(ns);
                assertEquals(ns + 11, state.value());
            }
            assertTrue(windowControlCount(views, 64) - lastHits > 240);
            byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.prepareSnapshot(engine);
            long lastWrites = windowControlCount(views, 104);
            for (long ns : windows) {
                state.setCurrentNamespace(ns);
                state.value();
                state.update(-ns - 11);
            }
            assertEquals(lastWrites, windowControlCount(views, 104));
            assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
            NativeEngine.releaseSnapshot(engine);
            // These two namespaces collide in the descriptor directory and use the same user key.
            state.setCurrentNamespace(34L); state.update(3400L);
            for (int i = 0; i < 50; i++) {
                state.setCurrentNamespace(0L); assertEquals(-11L, state.value());
                state.setCurrentNamespace(34L); assertEquals(3400L, state.value());
            }
            state.clear(); assertNull(state.value());
            state.setCurrentNamespace(0L); assertEquals(-11L, state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void aRecurringSmallWindowCanHitOnItsFirstPositiveRead() throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0ValueState<Long, Long, Long> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (long ns = 0; ns < 8; ns++) {
                state.setCurrentNamespace(ns);
                for (long key = 0; key < 3; key++) { ctx.currentKey = key; state.update(ns + key); }
                long before = windowControlCount(views, 64);
                ctx.currentKey = 2L; assertEquals(ns + 2, state.value());
                if (ns >= 4) {
                    assertTrue(windowControlCount(views, 64) > before);
                    int cell = (int) ((ns * 0x9e3779b97f4a7c15L) >>> 62) * NamespaceReadIndex.CONTROL_BYTES;
                    assertEquals(3L, views.getLong(cell + 8));
                    ctx.currentKey = 1L; state.clear(); assertNull(state.value());
                    ctx.currentKey = 0L; assertEquals(ns, state.value());
                    ctx.currentKey = 2L; assertEquals(ns + 2, state.value());
                    ctx.currentKey = 1L; state.update(ns + 1); assertEquals(ns + 1, state.value());
                }
                for (long key = 0; key < 3; key++) { ctx.currentKey = key; state.clear(); }
                assertNull(state.value());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentRowUpdatesValidateTheNamespaceSlotLengthAndSnapshotBarrier() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, rows);
            ForL0KeyContext<Long> ctx = context(11L);
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(
                    handle, ctx, LongSerializer.INSTANCE, LongSerializer.INSTANCE, rows, null);
            ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (long ns : new long[]{41, 42}) {
                state.setCurrentNamespace(ns);
                for (long key : new long[]{11, 12}) {
                    ctx.currentKey = key;
                    state.update(row(key, "original"));
                }
            }
            ctx.currentKey = 11L;
            state.setCurrentNamespace(41L);
            for (String text : Arrays.asList("x", "wide".repeat(80))) {
                state.update(row(9, text));
                long count = windowControlCount(controls, 104);
                for (int i = 0; i < 300; ++i) {
                    assertEquals(i == 0 ? 9 : i - 1, state.value().getLong(0));
                    state.update(row(i, text));
                }
                assertTrue(windowControlCount(controls, 104) - count > 200);
                state.value();
                ctx.currentKey = 12L;
                state.update(row(1000, "other"));
                state.setCurrentNamespace(42L);
                ctx.currentKey = 11L;
                assertEquals(11, state.value().getLong(0));
                state.setCurrentNamespace(41L);
                assertEquals(299, state.value().getLong(0));
                byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.prepareSnapshot(engine);
                count = windowControlCount(controls, 104);
                for (int i = 0; i < 140; ++i) {
                    state.value();
                    state.update(row(-i, text));
                }
                assertEquals(count, windowControlCount(controls, 104));
                assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
                NativeEngine.releaseSnapshot(engine);
                for (int i = 0; i < 140; ++i) {
                    state.value();
                    state.update(row(i + 7, text));
                }
                assertTrue(windowControlCount(controls, 104) > count);
                state.value();
                state.clear();
                assertNull(state.value());
                NativeEngine.readStateKeyGroupEntries(handle, 0, original);
                assertEquals(299, state.value().getLong(0));
                assertEquals(text, state.value().getString(1).toString());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    private <N> void checkMissingNamespace(TypeSerializer<N> serializer, N first, N second) throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, serializer, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> ctx = context(7L);
            ForL0ValueState<Long, N, Long> state = new ForL0ValueState<>(handle, ctx, LongSerializer.INSTANCE,
                    serializer, LongSerializer.INSTANCE, 91L);
            ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            state.setCurrentNamespace(first);
            for (int i = 0; i < 96; i++) { ctx.currentKey = (long)i; assertEquals(91L, state.value()); }
            assertEquals(95, windowControlCount(views, 112));
            assertEquals(0, windowControlCount(views, 40));
            ctx.currentKey = 7L;
            ctx.currentKeyGroupIndex = 1;
            state.update(41L);
            ctx.currentKeyGroupIndex = 0;
            assertEquals(91L, state.value());
            state.setCurrentNamespace(second);
            assertEquals(91L, state.value());
            state.update(21L);
            state.setCurrentNamespace(first);
            assertEquals(91L, state.value());
            state.update(31L);
            assertEquals(31L, state.value());
            NativeEngine.prepareSnapshot(engine);
            state.clear();
            for (int i = 0; i < 128; i++) assertEquals(91L, state.value());
            state.setCurrentNamespace(second);
            assertEquals(21L, state.value());
            byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear();
            NativeEngine.readStateKeyGroupEntries(handle, 0, original);
            assertEquals(21L, state.value());
            state.setCurrentNamespace(first);
            assertEquals(31L, state.value());
            ctx.currentKeyGroupIndex = 1;
            assertEquals(41L, state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void missingWindowsAreInvalidatedBeforeCreationAndRestore() throws Exception {
        checkMissingNamespace(LongSerializer.INSTANCE, Long.MIN_VALUE, 0L);
        checkMissingNamespace(new TimeWindow.Serializer(), new TimeWindow(10, 20), new TimeWindow(10, 21));
    }

    @Test void residentPackedTagsTrackEverySlotAcrossCollisionsAndCompaction() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = rowSerializer();
            MapSerializer<RowData, Integer> serializer = new MapSerializer<>(keys, IntSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, context(81L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, keys, IntSerializer.INSTANCE);
            BinaryRowData[] rows = new BinaryRowData[65];
            for (int i = 0; i < rows.length; i++) {
                // Odd keys have different unsampled bytes and the same sampled words and size.
                String text = i % 2 == 0 ? "s" : "a".repeat(40) + (char) ('A' + i) + "x".repeat(168);
                rows[i] = keys.toBinaryRow(row(i % 2 == 0 ? i : 3, text)).copy();
                if (i < 64) state.put(rows[i], -91 - i);
            }
            ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int round = 0; round < 32; round++) {
                for (int i = 0; i < 64; i++) assertEquals(-91 - i, state.get(rows[i]));
                assertNull(state.get(rows[64]));
            }
            assertTrue(view.getLong(64) > 1900);
            NativeEngine.prepareSnapshot(engine);
            for (int i = 1; i < 64; i += 2) state.remove(rows[i]);
            state.put(rows[64], 601);
            for (int i = 0; i < 64; i++) assertEquals(i % 2 == 0 ? Integer.valueOf(-91 - i) : null, state.get(rows[i]));
            assertEquals(601, state.get(rows[64]));
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear();
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            for (int i = 0; i < 64; i++) assertEquals(-91 - i, state.get(rows[i]));
            assertNull(state.get(rows[64]));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentJoinEnumerationOwnsSerializedKeysAndSupportsIteratorRemoval() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = rowSerializer();
            MapSerializer<RowData, Integer> serializer = new MapSerializer<>(keys, IntSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, context(44L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, keys, IntSerializer.INSTANCE);
            ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int i = 0; i < 8; i++) state.put(row(i, i % 2 == 0 ? "short" : "external".repeat(20)), -i);
            long before = views.getLong(64);
            for (int i = 0; i < 140; i++) {
                assertFalse(state.isEmpty());
                assertTrue(state.iterator().hasNext());
            }
            assertTrue(views.getLong(64) > before + 200);
            Iterable<Map.Entry<RowData, Integer>> held = state.entries();
            java.util.Iterator<Map.Entry<RowData, Integer>> live = state.iterator();
            Map.Entry<RowData, Integer> first = live.next();
            live.remove();
            assertNull(state.get(first.getKey()));
            assertThrows(IllegalStateException.class, live::remove);
            state.clear(); assertTrue(state.isEmpty());
            for (int pass = 0; pass < 2; pass++) {
                java.util.Set<Long> seen = new java.util.HashSet<>();
                for (Map.Entry<RowData, Integer> e : held) {
                    long n = e.getKey().getLong(0);
                    assertTrue(seen.add(n)); assertEquals(-(int)n, e.getValue());
                    assertEquals(n % 2 == 0 ? "short" : "external".repeat(20), e.getKey().getString(1).toString());
                }
                assertEquals(8, seen.size());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentRankEnumerationRetainsAllListFramingAfterMapClear() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> serializer = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle, context(17L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, LongSerializer.INSTANCE, lists);
            state.put(0L, java.util.Collections.emptyList());
            state.put(-1L, Arrays.asList(row(1, "x"), row(2, "external".repeat(60))));
            for (int i = 0; i < 64; i++) assertFalse(state.isEmpty());
            Iterable<Map.Entry<Long, List<RowData>>> held = state.entries();
            state.clear(); assertTrue(state.isEmpty());
            Map<Long, List<RowData>> snapshot = new HashMap<>();
            for (Map.Entry<Long, List<RowData>> entry : held) snapshot.put(entry.getKey(), entry.getValue());
            assertEquals(2, snapshot.size()); assertTrue(snapshot.get(0L).isEmpty());
            assertEquals(2L, snapshot.get(-1L).get(1).getLong(0));
            assertEquals("external".repeat(60), snapshot.get(-1L).get(1).getString(1).toString());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentMapListsOwnReadResultsAndPreserveFlinkFramingAcrossUpdates() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> serializer = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0KeyContext<Long> ctx = context(88L);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle, ctx,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, LongSerializer.INSTANCE, lists);
            ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int i = 0; i < 8; i++) state.put((long) i, java.util.Collections.singletonList(row(i, "start")));
            for (String text : Arrays.asList("x", "external".repeat(34))) {
                state.put(5L, java.util.Collections.singletonList(row(123, text)));
                long before = view.getLong(104);
                for (int i = 0; i < 200; i++) {
                    assertTrue(state.contains(5L));
                    List<RowData> result = state.get(5L);
                    assertEquals(i == 0 ? 123 : i - 1, result.get(0).getLong(0));
                    assertEquals(text, result.get(0).getString(1).toString());
                    ((BinaryRowData) result.get(0)).setLong(0, -12345);
                    assertNotEquals(-12345L, state.get(5L).get(0).getLong(0));
                    state.put(5L, java.util.Collections.singletonList(row(i, text)));
                }
                assertTrue(view.getLong(104) - before > 150);
                List<RowData> held = state.get(5L);
                byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.prepareSnapshot(engine);
                before = view.getLong(104);
                state.get(5L);
                state.put(5L, java.util.Collections.singletonList(row(1001, text)));
                state.put(0L, Arrays.asList(row(41, "one"), row(42, "two")));
                assertEquals(before, view.getLong(104));
                assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
                NativeEngine.releaseSnapshot(engine);
                assertEquals(199L, held.get(0).getLong(0));
                assertEquals(1001L, state.get(5L).get(0).getLong(0));
                assertEquals(42L, state.get(0L).get(1).getLong(0));
                state.get(5L); ctx.currentKey = 89L;
                state.put(5L, java.util.Collections.emptyList());
                assertTrue(state.get(5L).isEmpty());
                ctx.currentKey = 88L; state.clear();
                NativeEngine.readStateKeyGroupEntries(handle, 0, original);
                assertEquals(199L, state.get(5L).get(0).getLong(0));
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void packedNumericMapCopiesOnlyTheSerializedValueAndRechecksItsInnerKey() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, String> serializer = new MapSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0MapState<Long, VoidNamespace, Long, String> state = new ForL0MapState<>(handle, context(77L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
            ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            state.put(2L, "ab"); state.put(3L, "cd");
            for (int i = 0; i < 150; i++) {
                assertEquals(i == 0 || (i & 1) == 1 ? "ab" : "ba", state.get(2L));
                state.put(2L, (i & 1) == 0 ? "ab" : "ba");
                assertFalse(state.contains(400L));
            }
            assertTrue(view.getLong(104) > 100);
            state.get(2L); state.remove(2L); state.put(4L, "ef");
            assertNull(state.get(2L)); assertEquals("cd", state.get(3L)); assertEquals("ef", state.get(4L));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentRowMapWritesRevalidateBothKeysAndHonorTheSnapshotBarrier() throws Exception {
        for (boolean variable : new boolean[]{false, true}) {
            RowDataSerializer keys = variable ? rowSerializer() : keySerializer();
            long engine = engine();
            try {
                MapSerializer<RowData, Integer> serializer = new MapSerializer<>(keys, IntSerializer.INSTANCE);
                long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
                ForL0KeyContext<Long> ctx = context(71L);
                ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, ctx,
                        LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, keys, IntSerializer.INSTANCE);
                ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
                BinaryRowData[] entries = new BinaryRowData[9];
                for (int i = 0; i < entries.length; i++) {
                    RowData input = variable
                            ? row(i, "packed".repeat(24) + i) : GenericRowData.of((long) i, i == 1 ? null : (long) -i);
                    entries[i] = keys.toBinaryRow(input).copy();
                    if (i < 8) state.put(entries[i], i);
                }
                for (int i = 0; i < 400; i++) {
                    int j = i & 7;
                    assertEquals(i < 8 ? j : -(i - 8), state.get(entries[j]));
                    state.put(entries[j], -i);
                    assertNull(state.get(entries[8]));
                }
                assertTrue(view.getLong(64) > 300);
                assertTrue(view.getLong(104) > 300);
                state.get(entries[0]);
                ctx.currentKey = 72L;
                state.put(entries[0], Integer.MIN_VALUE);
                ctx.currentKey = 71L;
                assertEquals(-392, state.get(entries[0]));
                state.remove(entries[0]);
                state.put(entries[8], Integer.MAX_VALUE);
                assertEquals(-399, state.get(entries[7]));
                byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.prepareSnapshot(engine);
                long writes = view.getLong(104);
                for (int i = 0; i < 200; i++) {
                    state.get(entries[7]);
                    state.put(entries[7], i);
                }
                assertEquals(writes, view.getLong(104));
                assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
                NativeEngine.releaseSnapshot(engine);
                assertEquals(199, state.get(entries[7]));
                state.clear();
                NativeEngine.readStateKeyGroupEntries(handle, 0, original);
                assertEquals(-399, state.get(entries[7]));
                assertNull(state.get(entries[0]));
                ctx.currentKey = 72L;
                assertEquals(Integer.MIN_VALUE, state.get(entries[0]));
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void residentWindowMapUpdatesKeepWholeEntriesAndFrozenValues() throws Exception {
        long engine = engine();
        try {
            TimeWindow.Serializer windows = new TimeWindow.Serializer();
            MapSerializer<TimeWindow, TimeWindow> serializer = new MapSerializer<>(windows, windows);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
            ForL0MapState<Long, VoidNamespace, TimeWindow, TimeWindow> state = new ForL0MapState<>(handle,
                    context(17L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, windows, windows);
            ByteBuffer view = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            TimeWindow one = new TimeWindow(-1, 91), two = new TimeWindow(-1, 92);
            state.put(one, new TimeWindow(-2, 3));
            state.put(two, new TimeWindow(-3, 4));
            for (int i = 0; i < 260; i++) {
                assertEquals(i == 0 ? new TimeWindow(-2, 3) : new TimeWindow(-i, i), state.get(one));
                state.put(one, new TimeWindow(-i - 1, i + 1));
            }
            assertTrue(view.getLong(104) > 200);
            state.get(one);
            state.put(two, new TimeWindow(1, 2));
            assertEquals(new TimeWindow(-260, 260), state.get(one));
            byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.prepareSnapshot(engine);
            state.get(one);
            state.put(one, new TimeWindow(-500, 600));
            state.remove(two);
            assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
            NativeEngine.releaseSnapshot(engine);
            assertEquals(new TimeWindow(-500, 600), state.get(one));
            assertNull(state.get(two));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void serializedRowMapKeysKeepFlinkBytesAtBothInlineWidthsAndWithVariableFields() throws Exception {
        for (RowDataSerializer keys : Arrays.asList(new RowDataSerializer(new BigIntType(), new BigIntType(), new BigIntType()),
                new RowDataSerializer(new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()), rowSerializer())) {
            long engine = engine();
            try {
                MapSerializer<RowData, Integer> serializer = new MapSerializer<>(keys, IntSerializer.INSTANCE);
                long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer);
                ForL0MapState<Long, VoidNamespace, RowData, Integer> map = new ForL0MapState<>(handle, context(19L),
                        LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, serializer, keys, IntSerializer.INSTANCE);
                List<BinaryRowData> stored = new java.util.ArrayList<>();
                for (long i = 0; i < 35; ++i) {
                    GenericRowData input;
                    if (keys.getArity() == 3) input = GenericRowData.of(i, null, -i);
                    else if (keys.getArity() == 4) input = GenericRowData.of(i, null, -i, 2L * i);
                    else input = (GenericRowData) row(i, "key".repeat((int) (i * 3)));
                    input.setRowKind(i % 2 == 0 ? org.apache.flink.types.RowKind.DELETE : org.apache.flink.types.RowKind.INSERT);
                    BinaryRowData key = keys.toBinaryRow(input).copy();
                    map.put(key, (int) -i);
                    stored.add(key);
                    assertEquals((int) -i, map.get(key));
                }
                byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                long iterator = NativeEngine.mapIteratorCreate(handle, 19, 0);
                NativeEngine.prepareSnapshot(engine);
                for (BinaryRowData key : stored) map.remove(key);
                assertTrue(map.isEmpty());
                byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.releaseSnapshot(engine);
                for (byte[] snapshot : Arrays.asList(original, frozen)) {
                    org.apache.flink.core.memory.DataInputDeserializer wire = new org.apache.flink.core.memory.DataInputDeserializer(snapshot);
                    assertEquals(1, wire.readInt());
                    VoidNamespaceSerializer.INSTANCE.deserialize(wire);
                    assertEquals(19L, wire.readLong());
                    assertEquals(wire.available() - 4, wire.readInt());
                    assertEquals(stored.size(), wire.readInt());
                    java.util.Set<RowData> unique = new java.util.HashSet<>();
                    while (wire.available() > 0) {
                        RowData key = keys.copy(keys.deserialize(wire));
                        int index = -wire.readInt();
                        assertEquals(stored.get(index), key);
                        assertTrue(unique.add(key));
                    }
                    assertEquals(stored.size(), unique.size());
                }
                NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
                try {
                    int count = 0;
                    byte[] entry;
                    while ((entry = NativeEngine.mapIteratorNext(iterator)) != null) {
                        org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(entry);
                        RowData key = keys.deserialize(in);
                        assertEquals(map.get(key), Integer.valueOf(in.readInt()));
                        assertEquals(0, in.available());
                        count++;
                    }
                    assertEquals(stored.size(), count);
                } finally { NativeEngine.mapIteratorDestroy(iterator); }
                for (int i = 0; i < stored.size(); ++i) assertEquals(-i, map.get(stored.get(i)));
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void editingReturnedRowsKeepsTheFrozenAndLiveValuesIndependent() throws Exception {
        RowDataSerializer rows = rowSerializer();
        for (boolean enabled : new boolean[]{false, true}) {
            long engine = NativeEngine.createEngine(0, 1, 1, enabled, 512 * 1024, true, 65536, 0, 16, 0, .875, 0);
            try {
                long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, rows);
                ForL0ValueState<Long, VoidNamespace, RowData> state = new ForL0ValueState<>(
                        handle, context(9L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, rows, null);
                for (String text : Arrays.asList("x", "wide".repeat(90))) {
                    state.update(row(11, text));
                    for (int i = 0; i < 256; ++i) assertEquals(11, state.value().getLong(0));
                    byte[] original = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                    BinaryRowData before = (BinaryRowData) state.value();
                    NativeEngine.prepareSnapshot(engine);
                    BinaryRowData during = (BinaryRowData) state.value();
                    before.setLong(0, 81);
                    assertEquals(11, during.getLong(0));
                    during.setLong(0, 93);
                    assertEquals(11, state.value().getLong(0));
                    state.update(during);
                    assertEquals(93, state.value().getLong(0));
                    assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
                    NativeEngine.releaseSnapshot(engine);
                    state.clear();
                    assertEquals(81, before.getLong(0));
                    assertEquals(text, during.getString(1).toString());
                }
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void rootBinaryRowsShareTheFlinkFormatForNumericFixedAndSerializedKeys() throws Exception {
        RowDataSerializer pair = new RowDataSerializer(new BigIntType(), new BigIntType());
        RowDataSerializer triple = new RowDataSerializer(new BigIntType(), new BigIntType(), new BigIntType());
        for (boolean enabled : new boolean[]{false, true}) {
            verifyRootBinaryRow(enabled, LongSerializer.INSTANCE, -97L, 103L);
            verifyRootBinaryRow(enabled, IntSerializer.INSTANCE, -97, 103);
            verifyRootBinaryRow(enabled, pair, GenericRowData.of(-9L, 7L),
                    GenericRowData.ofKind(org.apache.flink.types.RowKind.DELETE, null, 0L));
            verifyRootBinaryRow(enabled, triple, GenericRowData.of(-9L, 7L, 5L), GenericRowData.of(0L, null, 9L));
            verifyRootBinaryRow(enabled, StringSerializer.INSTANCE, "one", "two");
        }
    }

    private <K> void verifyRootBinaryRow(boolean enabled, TypeSerializer<K> keySerializer, K key, K other) throws Exception {
        long engine = NativeEngine.createEngine(0, 1, 1, enabled, 512 * 1024, true, 65536, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer values = rowSerializer();
            long handle = register(engine, 0, keySerializer, VoidNamespaceSerializer.INSTANCE, values);
            ForL0KeyContext<K> ctx = context(key);
            ctx.currentKeyGroupIndex = 0;
            ForL0ValueState<K, VoidNamespace, RowData> state = new ForL0ValueState<>(
                    handle, ctx, keySerializer, VoidNamespaceSerializer.INSTANCE, values, null);
            String text = "长行".repeat(100);
            RowData original = row(-1, text);
            org.apache.flink.core.memory.DataOutputSerializer fixture = new org.apache.flink.core.memory.DataOutputSerializer(256);
            fixture.writeInt(1);
            VoidNamespaceSerializer.INSTANCE.serialize(VoidNamespace.INSTANCE, fixture);
            if (keySerializer instanceof StringSerializer) {
                org.apache.flink.core.memory.DataOutputSerializer serializedKey = new org.apache.flink.core.memory.DataOutputSerializer(32);
                keySerializer.serialize(key, serializedKey);
                fixture.writeInt(serializedKey.length());
                fixture.write(serializedKey.getCopyOfBuffer());
            } else {
                keySerializer.serialize(key, fixture);
            }
            values.serialize(original, fixture);
            NativeEngine.readStateKeyGroupEntries(handle, 0, fixture.getCopyOfBuffer());
            assertEquals(text, state.value().getString(1).toString());
            BinaryRowData binary = values.toBinaryRow(original);
            byte[] raw = RowDataKeyAccessor.extractBinaryRowDataBytes(binary);
            int part = (raw.length + 15) / 16 * 8;
            MemorySegment first = MemorySegmentFactory.wrap(Arrays.copyOfRange(raw, 0, part));
            MemorySegment second = MemorySegmentFactory.wrap(Arrays.copyOfRange(raw, part, 2 * part));
            BinaryRowData split = new BinaryRowData(2);
            split.pointTo(new MemorySegment[]{first, second}, 0, raw.length);
            state.update(split);
            for (int i = 0; i < 256; i++) assertEquals(-1, state.value().getLong(0));
            if (enabled && (keySerializer instanceof LongSerializer || keySerializer instanceof IntSerializer
                    || key instanceof RowData && ((RowData) key).getArity() == 2)) {
                assertTrue(NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder()).getLong(64) > 100);
            }
            NativeEngine.prepareSnapshot(engine);
            state.update(state.value());
            state.update(row(12, "x"));
            assertArrayEquals(fixture.getCopyOfBuffer(), NativeEngine.writeStateKeyGroupEntries(handle, 0));
            NativeEngine.releaseSnapshot(engine);
            RowData borrowed = state.value();
            ctx.currentKey = other;
            state.update(borrowed);
            assertEquals("x", state.value().getString(1).toString());
            if (keySerializer instanceof RowDataSerializer) {
                org.apache.flink.core.memory.DataInputDeserializer in =
                        new org.apache.flink.core.memory.DataInputDeserializer(NativeEngine.getStateKeys(handle));
                assertEquals(2, in.readInt());
                java.util.Set<K> actual = new java.util.HashSet<>();
                for (int i = 0; i < 2; i++) {
                    byte[] entry = new byte[in.readInt()];
                    in.readFully(entry);
                    actual.add(keySerializer.deserialize(new org.apache.flink.core.memory.DataInputDeserializer(entry)));
                }
                RowDataSerializer keys = (RowDataSerializer) keySerializer;
                assertTrue(actual.contains(keys.toBinaryRow((RowData) key).copy()));
                assertTrue(actual.contains(keys.toBinaryRow((RowData) other).copy()));
            }
            state.clear();
            assertNull(state.value());
            ctx.currentKey = key;
            NativeEngine.readStateKeyGroupEntries(handle, 0, fixture.getCopyOfBuffer());
            assertEquals(text, state.value().getString(1).toString());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void windowListsRoundTripLegacyBytesForNumericAndVariablePrimaryKeys() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (boolean numeric : new boolean[]{false, true}) {
            long legacy = engine(), current = engine();
            try {
                RowDataSerializer keys = numeric ? new RowDataSerializer(new BigIntType()) : rowSerializer();
                ListSerializer<Long> values = new ListSerializer<>(LongSerializer.INSTANCE);
                byte[] oldDescriptor = TypeAnalyzer.generateStateDescriptor(keys, LongSerializer.INSTANCE, values);
                boolean foundFlag = false;
                for (int i = 0; i < oldDescriptor.length - 2; i++) {
                    if ((oldDescriptor[i] & 255) == 0xc7 && (oldDescriptor[i + 2] & 255) == 0xc8) {
                        oldDescriptor[i + 1] &= ~4; foundFlag = true; break;
                    }
                }
                assertTrue(foundFlag);
                long old = NativeEngine.registerState(legacy, "saved", 1, TypeAnalyzer.getTypeId(keys),
                        TypeAnalyzer.getTypeId(values), TypeAnalyzer.TYPE_INT64, oldDescriptor);
                long handle = register(current, 1, keys, LongSerializer.INSTANCE, values);
                assertFalse(NativeEngine.listUsesLongNamespaces(old));
                assertTrue(NativeEngine.listUsesLongNamespaces(handle));
                RowData key = numeric ? GenericRowData.of((Object) null) : row(-815L, "variable key");
                key.setRowKind(org.apache.flink.types.RowKind.UPDATE_BEFORE);
                ForL0KeyContext<RowData> ctx = context(key);
                ForL0ListState<RowData, Long, Long> saved = new ForL0ListState<>(old, ctx, keys,
                        LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
                ForL0ListState<RowData, Long, Long> live = new ForL0ListState<>(handle, ctx, keys,
                        LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
                saved.setCurrentNamespace(Long.MIN_VALUE); live.setCurrentNamespace(Long.MIN_VALUE);
                saved.add(-90L); saved.addAll(Arrays.asList(0L, Long.MAX_VALUE));
                NativeEngine.prepareSnapshot(legacy);
                byte[] original = NativeEngine.writeStateKeyGroupEntries(old, 0);
                NativeEngine.releaseSnapshot(legacy);
                NativeEngine.readStateKeyGroupEntries(handle, 0, original);
                for (int i = 0; i < 200; ++i) assertEquals(Arrays.asList(-90L, 0L, Long.MAX_VALUE), live.getInternal());
                if (numeric) {
                    java.nio.ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
                    assertTrue(windowControlCount(controls, 64) > 50);
                    ctx.currentKey = GenericRowData.of(0L);
                    assertNull(live.getInternal()); ctx.currentKey = key;
                }
                byte[] enumerated = NativeEngine.getWindowStateKeys(handle, false, Long.MIN_VALUE, 0);
                org.apache.flink.core.memory.DataInputDeserializer in = new org.apache.flink.core.memory.DataInputDeserializer(enumerated);
                assertEquals(1, in.readInt()); assertEquals(key.getRowKind(), keys.deserialize(in).getRowKind()); assertEquals(0, in.available());
                org.apache.flink.core.memory.DataOutputSerializer combined = new org.apache.flink.core.memory.DataOutputSerializer(64);
                combined.writeLong(Long.MIN_VALUE); keys.serialize(key, combined);
                NativeEngine.prepareSnapshot(current);
                live.clear(); live.add(71L);
                assertArrayEquals(original, NativeEngine.writeStateKeyGroupEntries(handle, 0));
                NativeEngine.releaseSnapshot(current);
                org.apache.flink.core.memory.DataOutputSerializer raw = new org.apache.flink.core.memory.DataOutputSerializer(32);
                raw.writeLong(72L);
                NativeEngine.listAddGeneric(handle, combined.getCopyOfBuffer(), 0, raw.getCopyOfBuffer());
                assertEquals(Arrays.asList(71L, 72L), live.getInternal());
                NativeEngine.prepareSnapshot(current);
                byte[] next = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.releaseSnapshot(current);
                saved.clear(); NativeEngine.readStateKeyGroupEntries(old, 0, next);
                assertEquals(Arrays.asList(71L, 72L), saved.getInternal());
            } finally { NativeEngine.destroyEngine(current); NativeEngine.destroyEngine(legacy); }
        }
    }

    @Test void inlineWindowListKeysUseTheSameBytesAsLegacyKeysAcrossRestore() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = new RowDataSerializer();
            ListSerializer<Long> values = new ListSerializer<>(LongSerializer.INSTANCE);
            long handle = register(engine, 1, keys, LongSerializer.INSTANCE, values);
            assertTrue(NativeEngine.listSupportsInlineWindowKeys(handle));
            ForL0KeyContext<RowData> ctx = context(GenericRowData.of());
            ForL0ListState<RowData, Long, Long> state = new ForL0ListState<>(handle, ctx, keys,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
            state.setCurrentNamespace(-17L);
            byte[] slice = new byte[25];
            slice[5] = 2; slice[8] = (byte) 0x9d; slice[12] = 71;
            BinaryRowData padded = new BinaryRowData(0);
            padded.pointTo(MemorySegmentFactory.wrap(slice), 5, 8);
            BinaryRowData extended = new BinaryRowData(0);
            extended.pointTo(MemorySegmentFactory.wrap(slice), 5, 16);
            RowData[] distinct = {GenericRowData.of(), GenericRowData.ofKind(org.apache.flink.types.RowKind.DELETE),
                    padded, extended};
            for (int i = 0; i < distinct.length; i++) {
                ctx.currentKey = distinct[i];
                state.add((long) i);
                state.addAll(Arrays.asList(30L, 40L));
                org.apache.flink.core.memory.DataOutputSerializer key = new org.apache.flink.core.memory.DataOutputSerializer(32);
                keys.serialize(ctx.currentKey, key);
                byte[] oldTransport = NativeEngine.listGetLongNamespace(handle, 0, -17L, 0, 0,
                        key.getSharedBuffer(), key.length());
                assertEquals(Arrays.asList((long) i, 30L, 40L), values.deserialize(
                        new org.apache.flink.core.memory.DataInputDeserializer(oldTransport)));
            }
            NativeEngine.prepareSnapshot(engine);
            for (RowData row : distinct) { ctx.currentKey = row; state.clear(); }
            byte[] before = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, before);
            for (int i = 0; i < distinct.length; i++) {
                ctx.currentKey = distinct[i];
                assertEquals(Arrays.asList((long) i, 30L, 40L), state.getInternal());
            }
            for (int length = 0; length <= 16; length++) {
                ByteBuffer encoded = ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < length; i++) encoded.put((byte) (3 + i + length));
                byte[] element = ByteBuffer.allocate(8).putLong(length).array();
                NativeEngine.listChangeLongNamespace(handle, 1, 19L, encoded.getLong(0), encoded.getLong(8),
                        null, ~length, 0, element, 0, element.length);
                byte[] saved = NativeEngine.listGetLongNamespace(handle, 1, 19L, 0, 0, encoded.array(), length);
                assertEquals(Arrays.asList((long) length), values.deserialize(
                        new org.apache.flink.core.memory.DataInputDeserializer(saved)));
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentWindowListsReturnOwnedRowsDuringReplacementAndNamespaceMerges() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            ListSerializer<RowData> values = new ListSerializer<>(rows);
            long handle = register(engine, 1, LongSerializer.INSTANCE, LongSerializer.INSTANCE, values);
            ForL0KeyContext<Long> ctx = context(-1L);
            ForL0ListState<Long, Long, RowData> state = new ForL0ListState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, rows, values, null);
            state.setCurrentNamespace(10L);
            state.add(row(19L, "original".repeat(60)));
            for (int i = 0; i < 256; ++i) assertEquals(19L, state.getInternal().get(0).getLong(0));
            List<RowData> held = state.getInternal();
            state.setCurrentNamespace(11L); state.add(row(21, "next"));
            state.mergeNamespaces(10L, java.util.Collections.singletonList(11L));
            assertNull(state.getInternal()); state.setCurrentNamespace(10L);
            assertEquals(2, state.getInternal().size());
            state.update(java.util.Collections.singletonList(row(23, "replacement")));
            for (long i = 0; i < 400; ++i) {
                state.setCurrentNamespace(100 + i);
                state.add(row(50 + i, "payload".repeat(16))); state.clear();
            }
            assertEquals(19L, held.get(0).getLong(0));
            assertEquals("original".repeat(60), held.get(0).getString(1).toString());
            state.setCurrentNamespace(10L);
            assertEquals(23L, state.getInternal().get(0).getLong(0));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentWindowMapReadsWholeKeysAcrossInlinePackedAndHashedMaps() throws Exception {
        long engine = engine();
        try {
            org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer windows =
                    new org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer();
            MapSerializer<org.apache.flink.streaming.api.windowing.windows.TimeWindow,
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow> map = new MapSerializer<>(windows, windows);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, map);
            ForL0KeyContext<Long> ctx = context(17L);
            ForL0MapState<Long, VoidNamespace, org.apache.flink.streaming.api.windowing.windows.TimeWindow,
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow> state = new ForL0MapState<>(handle, ctx,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, map, windows, windows);
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int n = 1; n <= 65; n++) {
                org.apache.flink.streaming.api.windowing.windows.TimeWindow key =
                        new org.apache.flink.streaming.api.windowing.windows.TimeWindow(-12, n);
                org.apache.flink.streaming.api.windowing.windows.TimeWindow value =
                        new org.apache.flink.streaming.api.windowing.windows.TimeWindow(-101, n + 100);
                state.put(key, value);
                long previous = views.getLong(64);
                for (int i = 0; i < 128; ++i) {
                    assertEquals(value, state.get(key)); assertTrue(state.contains(key));
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow missing =
                            new org.apache.flink.streaming.api.windowing.windows.TimeWindow(-12, n + 1);
                    assertNull(state.get(missing)); assertFalse(state.contains(missing));
                }
                if (n <= 64) assertTrue(views.getLong(64) > previous);
                else assertEquals(previous, views.getLong(64));
            }
            NativeEngine.prepareSnapshot(engine);
            state.clear();
            byte[] old = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, old);
            for (int n = 1; n <= 65; n++) {
                org.apache.flink.streaming.api.windowing.windows.TimeWindow key =
                        new org.apache.flink.streaming.api.windowing.windows.TimeWindow(-12, n);
                assertEquals(n + 100L, state.get(key).getEnd());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentNumberMapsReadIndependentInnerAndOuterKeys() throws Exception {
        long engine = engine();
        try {
            LongSerializer numbers = LongSerializer.INSTANCE;
            MapSerializer<Long, Long> map = new MapSerializer<>(numbers, numbers);
            long handle = register(engine, 2, numbers, VoidNamespaceSerializer.INSTANCE, map);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0MapState<Long, VoidNamespace, Long, Long> state = new ForL0MapState<>(handle, ctx,
                    numbers, VoidNamespaceSerializer.INSTANCE, map, numbers, numbers);
            for (int g = 0; g < 2; g++) for (long k = 0; k < 24; k++) {
                ctx.currentKeyGroupIndex = g; ctx.currentKey = k;
                for (long i = 0; i <= k % 16; i++) state.put(i, i == 0 ? Long.MIN_VALUE : 10000L * g + 100L * k + i);
            }
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            for (int repeat = 0; repeat < 48; repeat++) for (int g = 0; g < 2; g++) for (long k = 0; k < 24; k++) {
                ctx.currentKeyGroupIndex = g; ctx.currentKey = k;
                assertEquals(Long.MIN_VALUE, state.get(0L).longValue());
                long i = k % 16; assertEquals(i == 0 ? Long.MIN_VALUE : 10000L * g + 100L * k + i, state.get(i).longValue());
                assertFalse(state.contains(-1L));
            }
            assertTrue(views.getLong(64) > 100 && views.getLong(NamespaceReadIndex.CONTROL_BYTES + 64) > 100);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void snapshotKeepsWindowRowsAfterSlotUpdatesAndTableRecreation() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (boolean cached : new boolean[]{false, true}) {
            long engine = cached ? engine()
                    : NativeEngine.createEngine(0, 2, 2, false, 0, false, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer values = rowSerializer();
                long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, values);
                ForL0KeyContext<Long> context = context(0L);
                ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, context,
                        LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
                state.setCurrentNamespace(17L);
                for (int group = 0; group < 2; group++) {
                    context.currentKeyGroupIndex = group;
                    for (long k = 0; k < 32; k++) {
                        context.currentKey = k;
                        state.update(row(1000L * group + k, "frozen-" + k));
                    }
                }
                NativeEngine.prepareSnapshot(engine);
                for (int group = 0; group < 2; group++) {
                    context.currentKeyGroupIndex = group;
                    for (long k = 0; k < 32; k++) {
                        context.currentKey = k;
                        assertEquals(1000L * group + k, state.value().getLong(0));
                        state.update(row(25, "changed")); state.clear();
                    }
                    for (long k = 0; k < 64; k++) {
                        context.currentKey = k;
                        state.update(row(32, "new")); state.clear();
                    }
                }
                byte[][] frozen = {NativeEngine.writeStateKeyGroupEntries(handle, 0),
                        NativeEngine.writeStateKeyGroupEntries(handle, 1)};
                NativeEngine.releaseSnapshot(engine);
                for (int group = 0; group < 2; group++) {
                    NativeEngine.readStateKeyGroupEntries(handle, group, frozen[group]);
                    context.currentKeyGroupIndex = group;
                    for (long k = 0; k < 64; k++) {
                        context.currentKey = k;
                        if (k >= 32) assertNull(state.value());
                        else {
                            RowData restored = state.value();
                            assertEquals(1000L * group + k, restored.getLong(0));
                            assertEquals("frozen-" + k, restored.getString(1).toString());
                        }
                    }
                }
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void largeRootTablesPublishIndependentReadViewsAndReturnTheirSpace() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 2L << 20, true, 65536, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer values = rowSerializer();
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, values);
            ForL0KeyContext<Long> ctx = context(7L);
            ForL0ValueState<Long, VoidNamespace, RowData> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, values, null);
            java.nio.ByteBuffer views = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            BinaryRowData[] data = new BinaryRowData[2];
            for (int g = 0; g < 2; g++) {
                data[g] = values.toBinaryRow(row(123 + g, (g == 0 ? "a" : "b").repeat(360 * 1024))).copy();
                ctx.currentKeyGroupIndex = g; state.update(data[g]);
                int slot = g * NamespaceReadIndex.CONTROL_BYTES + 48;
                for (int i = 0; i < 1500 && views.getLong(slot) == 0; ++i) {
                    assertEquals(123 + g, state.value().getLong(0)); state.update(data[g]);
                }
                assertTrue(views.getLong(slot) != 0);
            }
            for (int i = 0; i < 256; i++) {
                int g = i & 1; ctx.currentKeyGroupIndex = g; assertEquals(123 + g, state.value().getLong(0));
            }
            assertTrue(views.getLong(64) > 80 && views.getLong(NamespaceReadIndex.CONTROL_BYTES + 64) > 80);
            NativeEngine.prepareSnapshot(engine);
            ctx.currentKeyGroupIndex = 0; state.clear();
            ctx.currentKeyGroupIndex = 1; assertEquals(124, state.value().getLong(0));
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            ctx.currentKeyGroupIndex = 0; assertEquals(123, state.value().getLong(0));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void overflowGroupDoesNotSuppressDirectReadsInItsResidentNeighbor() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        long engine = NativeEngine.createEngine(0, 2, 2, true, 256 * 1024, true, 32768, 0, 16, 0, .875, 0);
        try {
            RowDataSerializer values = rowSerializer();
            long handle = register(engine, 0, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, values);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0ValueState<Long, VoidNamespace, RowData> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, values, null);
            String text = "payload".repeat(24);
            for (int g = 0; g < 2; g++) {
                ctx.currentKeyGroupIndex = g;
                for (long k = 0; k < (g == 0 ? 8 : 8192); k++) {
                    ctx.currentKey = k; state.update(row(k + 10000L * g, text));
                }
            }
            java.nio.ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            long before = controls.getLong(64);
            for (long i = 0; i < 4096; i++) {
                int g = (int) (i & 1); ctx.currentKeyGroupIndex = g; ctx.currentKey = i & (g == 0 ? 7 : 8191);
                assertEquals(ctx.currentKey + 10000L * g, state.value().getLong(0));
            }
            assertTrue(controls.getLong(64) > before + 1200);
            ctx.currentKeyGroupIndex = 1;
            for (long k = 9000; k < 11000; k++) { ctx.currentKey = k; assertNull(state.value()); }
            ctx.currentKey = 10000L; state.update(row(-7, text));
            assertEquals(-7, state.value().getLong(0));
            state.clear(); assertNull(state.value());
            ctx.currentKeyGroupIndex = 0; ctx.currentKey = 0L; assertEquals(0, state.value().getLong(0));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void mapMissSamplingResumesWhenAStreamOfExistingKeysTurnsSparse() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, Long> maps = new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0MapState<Long, VoidNamespace, Long, Long> state = new ForL0MapState<>(handle, ctx,
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            for (long i = 0; i < 16; i++) { ctx.currentKey = i; state.put(4L, 2 * i); }
            ctx.currentKey = 333333L;
            assertNull(state.get(4L));
            NamespaceReadIndex probe = NamespaceReadIndex.forMap(handle, 0);
            for (int i = 0; i < 64; i++) assertFalse(probe.absentNumber(0, i % 16));
            for (int i = 0; i < 256; i++) assertFalse(probe.absentNumber(0, 9000 + i));
            int proven = 0;
            for (int i = 0; i < 256; i++) if (probe.absentNumber(0, 9000 + i)) ++proven;
            assertTrue(proven > 100);
            for (long i = 0; i < 128; i++) {
                ctx.currentKey = 9000 + i;
                assertNull(state.get(4L));
                state.put(4L, -i);
                assertEquals(-i, state.get(4L));
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void rankMapEditsRetainListsAcrossGroupsRehashAndSnapshot() throws Exception {
        for (boolean enabled : new boolean[]{false, true}) {
            long engine = NativeEngine.createEngine(0, 2, 2, enabled, 256 * 1024, true, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer rows = rowSerializer();
                ListSerializer<RowData> lists = new ListSerializer<>(rows);
                MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
                long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
                ForL0KeyContext<Long> ctx = context(0L);
                ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(
                        handle, ctx, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, lists);
                for (int i = 0; i < 800; i++) {
                    ctx.currentKeyGroupIndex = i % 2;
                    ctx.currentKey = (long) (i % 23);
                    state.put(1L, Arrays.asList(row(i, "a"), row(-i, null)));
                    state.put(2L, Arrays.asList(row(17, "discard")));
                    state.remove(2L);
                }
                ctx.currentKey = 7L;
                ctx.currentKeyGroupIndex = 1;
                List<RowData> old = state.get(1L);
                long frozenValue = old.get(0).getLong(0);
                NativeEngine.prepareSnapshot(engine);
                state.remove(1L);
                state.put(19L, Arrays.asList(row(19, "new")));
                for (long i = 100; i < 1400; i++) {
                    ctx.currentKey = i;
                    state.put(1L, Arrays.asList(row(i, "grows")));
                }
                byte[] checkpoint = NativeEngine.writeStateKeyGroupEntries(handle, 1);
                NativeEngine.releaseSnapshot(engine);
                NativeEngine.readStateKeyGroupEntries(handle, 1, checkpoint);
                ctx.currentKey = 7L;
                assertNull(state.get(19L));
                assertEquals(frozenValue, state.get(1L).get(0).getLong(0));
                assertTrue(state.get(1L).get(1).isNullAt(1));
                ctx.currentKeyGroupIndex = 0;
                assertNotEquals(frozenValue, state.get(1L).get(0).getLong(0));
                assertEquals("a", old.get(0).getString(1).toString());
            } finally { NativeEngine.destroyEngine(engine); }
        }
    }

    @Test void pairedWindowEditsSurviveInterveningKeysAndCheckpointRestore() throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0ValueState<Long, Long, Long> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            java.nio.ByteBuffer controls = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            Map<Long, Long> expected = new HashMap<>();
            state.setCurrentNamespace(12L);
            for (long i = 0; i < 32; i++) { ctx.currentKey = i; state.update(i); expected.put(i, i); }
            for (long i = 0; i < 1024; i++) {
                long key = i % 32, temporary = -i - 1;
                ctx.currentKey = key;
                assertEquals(expected.get(key), state.value());
                if (i % 4 == 0) ctx.currentKey = (key + 1) % 32;
                state.update(i * 7);
                expected.put(ctx.currentKey, i * 7);
                ctx.currentKey = temporary;
                assertNull(state.value());
                state.update(i);
                assertEquals(i, state.value());
                state.clear();
            }
            assertTrue(windowControlCount(controls, 88) + windowControlCount(controls, 104) > 200);
            assertTrue(windowControlCount(controls, 104) > 100);
            assertTrue(windowControlCount(controls, 96) > 100);
            NativeEngine.prepareSnapshot(engine);
            for (long key : expected.keySet()) {
                ctx.currentKey = key;
                assertEquals(expected.get(key), state.value());
                state.clear();
            }
            byte[] checkpoint = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, checkpoint);
            for (Map.Entry<Long, Long> row : expected.entrySet()) {
                ctx.currentKey = row.getKey();
                assertEquals(row.getValue(), state.value());
            }
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentWindowControlsProveMissesAcrossGrowthExpiryAndRestore() throws Exception {
        long engine = engine();
        try {
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            ForL0KeyContext<Long> ctx = context(0L);
            ForL0ValueState<Long, Long, Long> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, LongSerializer.INSTANCE, null);
            java.nio.ByteBuffer cells = NativeEngine.residentControlViews(handle).order(java.nio.ByteOrder.nativeOrder());
            Map<Long, Map<Long, Long>> expected = new HashMap<>();
            java.util.Random random = new java.util.Random(1799);
            for (int batch = 0; batch < 48; batch++) {
                long ns = batch % 3 - 1;
                int group = batch % 2;
                ctx.currentKeyGroupIndex = group;
                state.setCurrentNamespace(ns);
                Map<Long, Long> rows = expected.computeIfAbsent(2 * ns + group, ignored -> new HashMap<>());
                for (int i = 0; i < 150; i++) {
                    long key = random.nextInt(240) - 120L;
                    ctx.currentKey = key;
                    assertEquals(rows.get(key), state.value());
                    if (i % 5 == 0) {
                        long value = random.nextLong();
                        state.update(value);
                        rows.put(key, value);
                        assertEquals(value, state.value());
                    } else if (i % 7 == 0) {
                        state.clear();
                        rows.remove(key);
                    }
                }
                if (batch % 8 == 0) {
                    for (long key : rows.keySet()) { ctx.currentKey = key; state.clear(); }
                    rows.clear();
                    ctx.currentKey = -999L;
                    state.update(ns);
                    rows.put(-999L, ns);
                    assertEquals(ns, state.value());
                }
            }
            assertTrue(windowControlCount(cells, 40) > 100);
            assertTrue(windowControlCount(cells, 64) > 100);
            ctx.currentKeyGroupIndex = 0;
            state.setCurrentNamespace(0L);
            ctx.currentKey = 1234567L;
            state.update(-77L);
            NativeEngine.prepareSnapshot(engine);
            state.clear();
            byte[] checkpoint = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            assertNull(state.value());
            NativeEngine.readStateKeyGroupEntries(handle, 0, checkpoint);
            assertEquals(-77L, state.value());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void listAppendAndRestorePreserveOwnedRows() throws Exception {
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
            List<RowData> singleton = state.getInternal();
            assertEquals(1, singleton.size());
            assertEquals("one", singleton.get(0).getString(1).toString());
            state.add(row(2, null));
            assertEquals(2, state.getInternal().size());
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

    @Test void largeListsAndRestoredListsKeepAllElements() throws Exception {
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
            assertEquals(input, state.getInternal());
            state.update(Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE));
            NativeEngine.prepareSnapshot(engine);
            byte[] snapshot = NativeEngine.writeKeyGroupData(engine, 0);
            NativeEngine.releaseSnapshot(engine);
            state.clear(); NativeEngine.readKeyGroupData(engine, 0, snapshot);
            List<Long> expected = Arrays.asList(Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE);
            assertEquals(expected, state.getInternal());
            assertEquals(expected, state.getInternal());
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

    @Test void outerMapAbsenceSurvivesInsertsIterationAndRestore() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, Long> maps = new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0KeyContext<Long> keys = context(0L);
            ForL0MapState<Long, VoidNamespace, Long, Long> state = new ForL0MapState<>(handle,
                    keys, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            for (int i = 0; i < 2600; ++i) {
                keys.setCurrentKeyGroupIndex(i & 1);
                keys.setCurrentKey(Long.MIN_VALUE + i);
                assertTrue(state.isEmpty());
                assertFalse(state.entries().iterator().hasNext());
                assertFalse(state.contains(0L));
                assertNull(state.get(0L));
                assertFalse(state.keys().iterator().hasNext());
                assertFalse(state.values().iterator().hasNext());
                state.put(0L, (long) i);
                state.put(1L, (long) ~i);
            }
            java.nio.ByteBuffer counters = NativeEngine.rootMembershipViews(handle).order(java.nio.ByteOrder.nativeOrder());
            assertTrue(counters.getLong(24) + counters.getLong(56) > 100);
            for (int i = 0; i < 2600; ++i) {
                keys.setCurrentKeyGroupIndex(i & 1);
                keys.setCurrentKey(Long.MIN_VALUE + i);
                assertFalse(state.isEmpty());
                assertTrue(state.contains(0L));
                assertEquals(Long.valueOf(i), state.get(0L));
                assertEquals(Long.valueOf(~i), state.get(1L));
            }
            keys.setCurrentKeyGroupIndex(0);
            keys.setCurrentKey(Long.MIN_VALUE + 52);
            NativeEngine.prepareSnapshot(engine);
            java.util.Iterator<java.util.Map.Entry<Long, Long>> iter = state.entries().iterator();
            Long removed = iter.next().getKey();
            iter.remove();
            assertFalse(state.contains(removed));
            state.clear();
            assertTrue(state.isEmpty());
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            assertEquals(Long.valueOf(52), state.get(0L));
            assertEquals(Long.valueOf(~52), state.get(1L));
            keys.setCurrentKey(Long.MAX_VALUE);
            java.util.Iterator<?> empty = state.entries().iterator();
            assertFalse(empty.hasNext());
            assertThrows(IllegalStateException.class, empty::remove);
            assertThrows(java.util.NoSuchElementException.class, empty::next);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericMapKeepsOtherEntriesOnMutation() throws Exception {
        long engine = engine();
        try {
            MapSerializer<Long, Long> maps = new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, Long> state = new ForL0MapState<>(handle,
                    context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            state.put(1L, 11L); state.put(2L, 22L);
            assertEquals(11L, state.get(1L));
            state.put(2L, 23L); assertEquals(11L, state.get(1L));
            state.remove(2L); assertEquals(11L, state.get(1L)); assertNull(state.get(2L));
            state.clear(); assertNull(state.get(1L));
            traffic(handle);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void rowMapKeysUseDirectAndSegmentedInputWithoutBorrowingTheStoredKey() throws Exception {
        checkMapKeyRanges(IntSerializer.INSTANCE, Integer.MIN_VALUE, Integer.MAX_VALUE);
        checkMapKeyRanges(DoubleSerializer.INSTANCE, -0.0, 2.75);
    }

    private <T> void checkMapKeyRanges(TypeSerializer<T> values, T first, T second) throws Exception {
        long engine = engine();
        try {
            RowDataSerializer keys = rowSerializer();
            MapSerializer<RowData,T> maps = new MapSerializer<>(keys, values);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, RowData, T> state = new ForL0MapState<>(handle, context(17L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, keys, values);
            BinaryRowData[] heap = new BinaryRowData[7];
            BinaryRowData[] direct = new BinaryRowData[7];
            for (int i = 0; i < heap.length; i++) {
                heap[i] = keys.toBinaryRow(row(i, i == 0 ? null : "测".repeat(i * 32))).copy();
                int length = heap[i].getSizeInBytes();
                MemorySegment segment = MemorySegmentFactory.wrapOffHeapMemory(
                        ByteBuffer.allocateDirect(length + 16));
                heap[i].getSegments()[0].copyTo(heap[i].getOffset(), segment, 8, length);
                direct[i] = new BinaryRowData(2);
                direct[i].pointTo(segment, 8, length);
                assertNull(state.get(direct[i]));
                state.put(direct[i], first);
                assertEquals(first, state.get(heap[i]));
                assertEquals(first, state.get(direct[i]));
            }
            NativeEngine.prepareSnapshot(engine);
            for (RowData key : direct) state.put(key, second);
            byte[] snapshot = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            direct[0].setLong(0, 1000L);
            assertNull(state.get(direct[0]));
            assertEquals(second, state.get(heap[0]));
            BinaryRowData source = heap[3];
            int length = source.getSizeInBytes(), part = (length + 1) / 2;
            MemorySegment[] segments = {
                    MemorySegmentFactory.wrap(new byte[part]),
                    MemorySegmentFactory.wrap(new byte[part])};
            source.getSegments()[0].copyTo(source.getOffset(), segments[0], 0, part);
            source.getSegments()[0].copyTo(source.getOffset() + part, segments[1], 0, length - part);
            BinaryRowData split = new BinaryRowData(2);
            split.pointTo(segments, 0, length);
            assertEquals(second, state.get(split));
            state.remove(split); assertFalse(state.contains(heap[3]));
            NativeEngine.readStateKeyGroupEntries(handle, 0, snapshot);
            for (RowData key : heap) assertEquals(first, state.get(key));
            assertEquals(first, state.get(split));
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void wideJoinKeysCompareAllBytesAtHeapAndUnsafeSegmentOffsets() throws Exception {
        long engine = engine();
        MemorySegment direct = null;
        try {
            RowDataSerializer rows = rowSerializer();
            MapSerializer<RowData, Integer> maps = new MapSerializer<>(rows, IntSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, context(1L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, rows, IntSerializer.INSTANCE);
            String repeated = "q".repeat(380);
            BinaryRowData first = rows.toBinaryRow(row(8, repeated)).copy();
            for (int i = 0; i < 8; i++) {
                String value = repeated.substring(0, 70 + i) + "x" + repeated.substring(71 + i);
                state.put(row(8, value), i);
            }
            state.put(first, -7);
            int length = first.getSizeInBytes();
            MemorySegment heap = MemorySegmentFactory.wrap(new byte[length + 13]);
            direct = MemorySegmentFactory.allocateOffHeapUnsafeMemory(length + 13);
            for (MemorySegment segment : Arrays.asList(heap, direct)) {
                first.getSegments()[0].copyTo(first.getOffset(), segment, 5, length);
                BinaryRowData probe = new BinaryRowData(2); probe.pointTo(segment, 5, length);
                for (int i = 0; i < 128; i++) assertEquals(-7, state.get(probe));
                for (int position : new int[]{0, 11, 105, length / 2, length - 1}) {
                    byte old = segment.get(5 + position);
                    segment.put(5 + position, (byte) (old ^ 0x40));
                    assertNull(state.get(probe));
                    segment.put(5 + position, old);
                    assertEquals(-7, state.get(probe));
                }
                state.put(probe, Integer.MIN_VALUE);
                assertEquals(Integer.MIN_VALUE, state.get(first));
                state.put(first, -7);
            }
        } finally {
            if (direct != null) direct.free();
            NativeEngine.destroyEngine(engine);
        }
    }

    @Test void residentKeyProjectionSkipsUnrequestedRankLists() throws Exception {
        long engine = engine();
        try {
            CountingRowSerializer rows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle, context(6L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, lists);
            for (long n = -4; n < 16; n++) state.put(n, Arrays.asList(row(n, "wide".repeat(40)), row(n, null)));
            for (int i = 0; i < 150; i++) assertNotNull(state.get(5L));
            int reads = rows.deserializations;
            Iterable<Long> held = state.keys();
            assertEquals(reads, rows.deserializations);
            state.clear();
            java.util.Set<Long> keys = new java.util.HashSet<>();
            held.forEach(keys::add);
            assertEquals(20, keys.size());
            for (long n = -4; n < 16; n++) assertTrue(keys.contains(n));
            assertFalse(state.keys().iterator().hasNext());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentCountAndWindowColumnsOwnTheirReturnedValues() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            MapSerializer<RowData, Integer> maps = new MapSerializer<>(rows, IntSerializer.INSTANCE);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, context(-7L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, rows, IntSerializer.INSTANCE);
            String large = "payload".repeat(30);
            state.put(row(8, large), Integer.MIN_VALUE);
            state.put(row(-2, null), Integer.MAX_VALUE);
            for (int i = 0; i < 120; i++) assertEquals(Integer.MIN_VALUE, state.get(row(8, large)));
            Iterable<RowData> keys = state.keys();
            Iterable<Integer> counts = state.values();
            state.clear();
            Map<Long, String> extracted = new HashMap<>();
            keys.forEach(k -> extracted.put(k.getLong(0), k.isNullAt(1) ? null : k.getString(1).toString()));
            assertEquals(2, extracted.size()); assertEquals(large, extracted.get(8L)); assertTrue(extracted.containsKey(-2L));
            java.util.Set<Integer> values = new java.util.HashSet<>(); counts.forEach(values::add);
            assertEquals(java.util.Set.of(Integer.MIN_VALUE, Integer.MAX_VALUE), values);
        } finally { NativeEngine.destroyEngine(engine); }
        engine = engine();
        try {
            TimeWindow.Serializer windows = new TimeWindow.Serializer();
            MapSerializer<TimeWindow, TimeWindow> maps = new MapSerializer<>(windows, windows);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, TimeWindow, TimeWindow> state = new ForL0MapState<>(handle, context(9L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, windows, windows);
            TimeWindow a = new TimeWindow(-8, 7), b = new TimeWindow(8, 10), c = new TimeWindow(12, 19);
            state.put(a, c); state.put(b, c);
            for (int i = 0; i < 120; i++) assertEquals(c, state.get(b));
            Iterable<TimeWindow> keys = state.keys(), values = state.values();
            state.clear();
            java.util.Set<TimeWindow> found = new java.util.HashSet<>(); keys.forEach(found::add);
            assertEquals(java.util.Set.of(a, b), found);
            List<TimeWindow> mapped = new java.util.ArrayList<>(); values.forEach(mapped::add);
            assertEquals(Arrays.asList(c, c), mapped);
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void residentListReadsReturnMutableIndependentRowsAndHonorSerializerChanges() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle, context(5L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, lists);
            NamespaceReadIndex index = NamespaceReadIndex.forMap(handle, 0);
            String wide = "retained".repeat(64);
            state.put(3L, Arrays.asList(row(5, wide), row(-8, null)));
            state.put(4L, java.util.Collections.emptyList());
            for (int i = 0; i < 128; i++) assertEquals(5, state.get(3L).get(0).getLong(0));
            assertEquals(NamespaceReadIndex.ROW, index.readMapPayload(0, 5L, 3L));
            List<RowData> held = state.get(3L);
            ((BinaryRowData) held.get(0)).setLong(0, 99);
            assertEquals(-8, held.get(1).getLong(0));
            held.remove(1);
            List<RowData> empty = state.get(4L);
            empty.add(row(42, null));
            assertTrue(state.get(4L).isEmpty());
            assertEquals(5, state.get(3L).get(0).getLong(0));

            byte[] serialized = NativeEngine.mapGetLongBytes(handle, 5L, 0, 3L);
            byte[] padded = Arrays.copyOf(serialized, serialized.length + 1);
            NativeEngine.mapPutLongBytesRange(handle, 5L, 0, 3L, padded, 0, padded.length);
            assertEquals(NamespaceReadIndex.ROW, index.readMapPayload(0, 5L, 3L));
            assertNull(index.copyCurrentRowList(2));
            assertEquals(2, state.get(3L).size());
            CountingRowSerializer custom = new CountingRowSerializer(new BigIntType(), new VarCharType());
            state.setUserValueSerializer(new ListSerializer<>(custom));
            assertTrue(state.get(3L).get(1).isNullAt(1));
            assertEquals(2, custom.deserializations);
            state.clear();
            assertEquals(99, held.get(0).getLong(0));
            assertEquals(wide, held.get(0).getString(1).toString());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void numericMapRowPayloadsMatchListSerializerForBorrowedRowsAndSchemaUpdates() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = rowSerializer();
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle, context(5L),
                    LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, LongSerializer.INSTANCE, lists);
            BinaryRowData source = rows.toBinaryRow(row(-13, "range".repeat(80))).copy();
            int length = source.getSizeInBytes();
            MemorySegment direct = MemorySegmentFactory.wrapOffHeapMemory(ByteBuffer.allocateDirect(length + 16));
            source.getSegments()[0].copyTo(source.getOffset(), direct, 8, length);
            BinaryRowData borrowed = new BinaryRowData(2); borrowed.pointTo(direct, 8, length);
            List<List<RowData>> variants = Arrays.asList(java.util.Collections.emptyList(),
                    java.util.Collections.singletonList(borrowed), Arrays.asList(source, row(0, null)));
            for (int i = 0; i < variants.size(); ++i) {
                org.apache.flink.core.memory.DataOutputSerializer expected = new org.apache.flink.core.memory.DataOutputSerializer(96);
                lists.serialize(variants.get(i), expected);
                state.put((long) i, variants.get(i));
                assertArrayEquals(expected.getCopyOfBuffer(), NativeEngine.mapGetLongBytes(handle, 5, 0, i));
            }
            borrowed.setLong(0, 999); assertEquals(-13, state.get(1L).get(0).getLong(0));
            NativeEngine.prepareSnapshot(engine);
            state.put(1L, java.util.Collections.singletonList(borrowed));
            byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, 0);
            NativeEngine.releaseSnapshot(engine);
            assertEquals(999, state.get(1L).get(0).getLong(0));
            state.clear(); NativeEngine.readStateKeyGroupEntries(handle, 0, frozen);
            assertEquals(-13, state.get(1L).get(0).getLong(0));
            CountingRowSerializer customRows = new CountingRowSerializer(new BigIntType(), new VarCharType());
            state.setUserValueSerializer(new ListSerializer<>(customRows));
            state.put(4L, java.util.Collections.singletonList(row(11, "custom")));
            assertEquals(1, customRows.serializations);
            assertEquals("custom", state.get(4L).get(0).getString(1).toString());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void recurringWindowRowsRemainIsolatedAcrossKeyGroupsExpiryAndSnapshot() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer values = rowSerializer();
            long handle = register(engine, 0, LongSerializer.INSTANCE, LongSerializer.INSTANCE, values);
            ForL0KeyContext<Long> ctx = context(100L);
            ForL0ValueState<Long, Long, RowData> state = new ForL0ValueState<>(handle, ctx,
                    LongSerializer.INSTANCE, LongSerializer.INSTANCE, values, null);
            ctx.currentKeyGroupIndex = 1; state.setCurrentNamespace(-1L); state.update(row(99, "anchor"));
            for (long window = 0; window < 13; ++window) {
                int group = (int) (window & 1);
                ctx.currentKeyGroupIndex = group;
                state.setCurrentNamespace(window);
                for (long k = 0; k < 20; ++k) {
                    ctx.currentKey = k; assertNull(state.value()); state.update(row(k + window, "value".repeat(16)));
                }
                if (window == 12) NativeEngine.prepareSnapshot(engine);
                for (long k = 0; k < 20; ++k) {
                    ctx.currentKey = k; assertEquals(k + window, state.value().getLong(0)); state.clear();
                }
                if (window == 12) {
                    byte[] frozen = NativeEngine.writeStateKeyGroupEntries(handle, group);
                    NativeEngine.releaseSnapshot(engine);
                    NativeEngine.readStateKeyGroupEntries(handle, group, frozen);
                    for (long k = 0; k < 20; ++k) {
                        ctx.currentKey = k; assertEquals(k + window, state.value().getLong(0));
                    }
                }
            }
            ctx.currentKeyGroupIndex = 1; ctx.currentKey = 100L; state.setCurrentNamespace(-1L);
            assertEquals("anchor", state.value().getString(1).toString());
        } finally { NativeEngine.destroyEngine(engine); }
    }

    @Test void packedRowKeyLookupsCompareTheEntireRowAcrossCollisionsAndRestore() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("forl0.test.l0"));
        for (boolean enabled : new boolean[]{false, true}) {
            long engine = NativeEngine.createEngine(0, 2, 2, enabled, 512 * 1024, true, 65536, 0, 16, 0, .875, 0);
            try {
                RowDataSerializer keys = new RowDataSerializer(new VarCharType());
                MapSerializer<RowData, Integer> maps = new MapSerializer<>(keys, IntSerializer.INSTANCE);
                long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
                ForL0MapState<Long, VoidNamespace, RowData, Integer> state = new ForL0MapState<>(handle, context(4L),
                        LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps, keys, IntSerializer.INSTANCE);
                BinaryRowData[] rows = new BinaryRowData[20];
                for (int i = 0; i < rows.length; ++i) {
                    String text = "p".repeat(24) + (char) ('A' + i) + "x".repeat(111);
                    rows[i] = keys.toBinaryRow(GenericRowData.of(StringData.fromString(text))).copy();
                }
                for (int i = 0; i < 16; ++i) {
                    state.put(rows[i], i);
                    for (int j = 0; j <= i; ++j) assertEquals(j, state.get(rows[j]));
                    assertNull(state.get(rows[19]));
                }
                NativeEngine.prepareSnapshot(engine);
                for (int i = 0; i < 5; i++) state.remove(rows[i]);
                for (int i = 5; i < rows.length; i++) state.put(rows[i], -i);
                byte[] snapshot = NativeEngine.writeStateKeyGroupEntries(handle, 0);
                NativeEngine.releaseSnapshot(engine);
                state.clear(); NativeEngine.readStateKeyGroupEntries(handle, 0, snapshot);
                for (int i = 0; i < 16; i++) assertEquals(i, state.get(rows[i]));
                for (int i = 16; i < rows.length; i++) assertNull(state.get(rows[i]));
            } finally { NativeEngine.destroyEngine(engine); }
        }
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

    @Test void listValuedMapsRefreshResidentValues() throws Exception {
        long engine = engine();
        try {
            RowDataSerializer rows = new RowDataSerializer(new BigIntType());
            ListSerializer<RowData> lists = new ListSerializer<>(rows);
            MapSerializer<Long, List<RowData>> maps = new MapSerializer<>(LongSerializer.INSTANCE, lists);
            long handle = register(engine, 2, LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps);
            ForL0MapState<Long, VoidNamespace, Long, List<RowData>> state = new ForL0MapState<>(handle,
                    context(1L), LongSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, maps,
                    LongSerializer.INSTANCE, lists);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            state.put(1L, Arrays.asList(GenericRowData.of(10L)));
            assertEquals(10L, state.get(1L).get(0).getLong(0));
            assertEquals(10L, state.entries().iterator().next().getValue().get(0).getLong(0));
            state.put(1L, Arrays.asList(GenericRowData.of(20L), GenericRowData.of(30L)));
            assertEquals(20L, state.get(1L).get(0).getLong(0));
            assertEquals(2, state.entries().iterator().next().getValue().size());
            state.remove(1L);
            assertNull(state.get(1L));
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
