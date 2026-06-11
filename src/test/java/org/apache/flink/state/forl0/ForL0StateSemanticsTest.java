package org.apache.flink.state.forl0;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Null-handling, default-value, and keyed-state iteration semantics for
 * {@link ForL0KeyedStateBackend}. Mirrors the subset of Flink's
 * {@code StateBackendTestBase} checks that are most likely to regress when
 * we change the HotCache integration — specifically:
 *
 * <ul>
 *   <li>{@code ValueState.value()} default is {@code null}; {@code update(null)}
 *       drops the entry (cache AND SwissTable must agree).</li>
 *   <li>{@code ListState.add(null)}, {@code addAll(null)},
 *       {@code addAll(listWithNull)}, {@code update(null)},
 *       {@code update(listWithNull)} all throw NPE.</li>
 *   <li>{@code MapState} allows null values but not null user keys.</li>
 *   <li>Multiple descriptors on the same backend do not bleed into each
 *       other (cache per state_id isolates attachment).</li>
 *   <li>{@code getKeys} / {@code applyToAllKeys} / {@code numKeyValueStateEntries}
 *       reflect the actual key set regardless of HotCache state.</li>
 *   <li>{@code clear()} removes the entry from BOTH the cache and the
 *       SwissTable — a cached stale hit would be a silent data-loss bug.</li>
 * </ul>
 */
public class ForL0StateSemanticsTest {

    private static ForL0KeyedStateBackend<Long> buildBackend() throws Exception {
        return buildBackend(/* l0Enabled */ false, /* l0Size */ 0L);
    }

    private static ForL0KeyedStateBackend<Long> buildBackend(boolean l0Enabled, long l0Size)
            throws Exception {
        KeyGroupRange kg = new KeyGroupRange(0, 7);
        ForL0KeyedStateBackendBuilder<Long> b = new ForL0KeyedStateBackendBuilder<>(
                null,
                LongSerializer.INSTANCE,
                Thread.currentThread().getContextClassLoader(),
                kg.getNumberOfKeyGroups(),
                kg,
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                LatencyTrackingStateConfig.disabled(),
                Collections.emptyList(),
                AbstractStateBackend.getCompressionDecorator(new ExecutionConfig()),
                new HeapPriorityQueueSetFactory(kg, kg.getNumberOfKeyGroups(), 128),
                true,
                l0Enabled,
                l0Size,
                new CloseableRegistry());
        return b.build();
    }

    // -----------------------------------------------------------------------
    //  ValueState: default value, null semantics
    // -----------------------------------------------------------------------

    @Test
    void valueStateDefaultIsNullForEmptyKey() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v", LongSerializer.INSTANCE));
            assertNull(state.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void valueStateAddAndGetLongAccumulates() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            @SuppressWarnings("unchecked")
            ForL0ValueState<Long, VoidNamespace, Long> state =
                    (ForL0ValueState<Long, VoidNamespace, Long>) backend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("v_add_and_get", LongSerializer.INSTANCE));

            assertEquals(1L, state.addAndGetLong(1L));
            assertEquals(Long.valueOf(1L), state.value());
            assertEquals(3L, state.addAndGetLong(2L));
            assertEquals(Long.valueOf(3L), state.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void valueStateUpdateNullClearsEntry() throws Exception {
        // Flink convention (see Heap/RocksDB backends): ValueState.update(null)
        // is an explicit clear. We must honour that AND invalidate the cache.
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            backend.setCurrentKey(42L);
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_null_clear", LongSerializer.INSTANCE));

            state.update(100L);
            assertEquals(Long.valueOf(100L), state.value());
            state.update(null);
            assertNull(state.value(), "update(null) must clear the entry");
            // Same-key re-read must still see null (cache must not report a stale hit).
            assertNull(state.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void valueStateClearRemovesFromBothCacheAndTable() throws Exception {
        // Writing k=X, reading (populates cache), clearing, re-reading must miss.
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            backend.setCurrentKey(7L);
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_clear", LongSerializer.INSTANCE));
            state.update(111L);
            assertEquals(Long.valueOf(111L), state.value()); // populates cache (if active)
            state.clear();
            assertNull(state.value());
            assertEquals(0, backend.numKeyValueStateEntries());
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  ListState null semantics
    // -----------------------------------------------------------------------

    @Test
    void listStateAddNullThrows() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ListState<String> list = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ListStateDescriptor<>("ls_null", StringSerializer.INSTANCE));
            assertThrows(NullPointerException.class, () -> list.add(null));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void listStateAddAllNullArgumentThrows() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ListState<String> list = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ListStateDescriptor<>("ls_addall_null", StringSerializer.INSTANCE));
            assertThrows(NullPointerException.class, () -> list.addAll(null));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void listStateAddAllListContainingNullThrows() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ListState<String> list = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ListStateDescriptor<>("ls_entries_null", StringSerializer.INSTANCE));
            List<String> withNull = Arrays.asList("a", null, "b");
            assertThrows(NullPointerException.class, () -> list.addAll(withNull));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void listStateUpdateNullArgumentThrows() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ListState<String> list = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ListStateDescriptor<>("ls_update_null", StringSerializer.INSTANCE));
            assertThrows(NullPointerException.class, () -> list.update(null));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void listStateUpdateEmptyClearsState() throws Exception {
        // update(empty) is the documented way to clear a ListState.
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            ListState<String> list = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ListStateDescriptor<>("ls_update_empty", StringSerializer.INSTANCE));
            list.add("x");
            list.add("y");
            list.update(Collections.emptyList());
            assertFalse(list.get().iterator().hasNext(), "list must be empty after update([])");
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  MapState null semantics
    // -----------------------------------------------------------------------

    @Test
    void mapStateAllowsNullValue() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(1L);
            MapState<String, String> map = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new MapStateDescriptor<>("map_null_val",
                            StringSerializer.INSTANCE, StringSerializer.INSTANCE));

            map.put("k1", "v1");
            map.put("k2", null);
            assertTrue(map.contains("k2"));
            assertNull(map.get("k2"));
            assertEquals("v1", map.get("k1"));
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  Multiple descriptors on one backend stay isolated
    // -----------------------------------------------------------------------

    @Test
    void multipleValueStatesAreIndependent() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            backend.setCurrentKey(5L);
            ValueState<Long> va = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("a", LongSerializer.INSTANCE));
            ValueState<Long> vb = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("b", LongSerializer.INSTANCE));

            va.update(1L);
            vb.update(2L);
            assertEquals(Long.valueOf(1L), va.value());
            assertEquals(Long.valueOf(2L), vb.value());

            va.clear();
            assertNull(va.value());
            // vb must still hold its value — caches are per-state.
            assertEquals(Long.valueOf(2L), vb.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void timeWindowValueStateReadsFixedRowDataWithoutSerializerFallback() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(3L);
            TypeSerializer<RowData> rowSerializer = createFixedRowDataSerializer();
            ValueState<RowData> state = backend.getPartitionedState(
                    new TimeWindow(10L, 20L),
                    new TimeWindow.Serializer(),
                    new ValueStateDescriptor<>("tw_row_value", rowSerializer));

            BinaryRowData expected = createFixedRowData(rowSerializer, 11L, 7);
            state.update(expected);

            RowData actual = state.value();
            assertTrue(actual instanceof BinaryRowData);
            assertEquals(11L, actual.getLong(0));
            assertEquals(7, actual.getInt(1));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void timeWindowReducingStateReadsFixedRowDataWithoutSerializerFallback() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(4L);
            TypeSerializer<RowData> rowSerializer = createFixedRowDataSerializer();
            ReducingState<RowData> state = backend.getPartitionedState(
                    new TimeWindow(30L, 40L),
                    new TimeWindow.Serializer(),
                    new ReducingStateDescriptor<>("tw_row_reduce", (left, right) -> right, rowSerializer));

            BinaryRowData expected = createFixedRowData(rowSerializer, 21L, 9);
            state.add(expected);

            RowData actual = state.get();
            assertTrue(actual instanceof BinaryRowData);
            assertEquals(21L, actual.getLong(0));
            assertEquals(9, actual.getInt(1));
        } finally {
            backend.dispose();
        }
    }

    @Test
    void reducingStateBuiltinLongSumAccumulatesAndClears() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(6L);
            ReducingState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ReducingStateDescriptor<>("reduce_long_sum", Long::sum, LongSerializer.INSTANCE));

            state.add(1L);
            state.add(2L);
            state.add(3L);

            assertEquals(Long.valueOf(6L), state.get());

            state.clear();
            assertNull(state.get());
        } finally {
            backend.dispose();
        }
    }

        @Test
        void reducingStateDetectsBuiltinLongReducersConservatively() {
            ReduceFunction<Long> longSum = Long::sum;
            ReduceFunction<Long> longMin = Long::min;
            ReduceFunction<Long> longMax = Math::max;
        ReduceFunction<Long> syntheticSum = (left, right) -> left + right;
            ReduceFunction<Integer> intSum = Integer::sum;

        assertEquals(
            ForL0ReducingState.BUILTIN_AGG_SUM,
                ForL0ReducingState.resolveBuiltinAggType(longSum, TypeAnalyzer.TYPE_INT64));
        assertEquals(
            ForL0ReducingState.BUILTIN_AGG_MIN,
                ForL0ReducingState.resolveBuiltinAggType(longMin, TypeAnalyzer.TYPE_INT64));
        assertEquals(
            ForL0ReducingState.BUILTIN_AGG_MAX,
                ForL0ReducingState.resolveBuiltinAggType(longMax, TypeAnalyzer.TYPE_INT64));
        assertEquals(
            ForL0ReducingState.BUILTIN_AGG_USER_DEFINED,
            ForL0ReducingState.resolveBuiltinAggType(syntheticSum, TypeAnalyzer.TYPE_INT64));
        assertEquals(
            ForL0ReducingState.BUILTIN_AGG_USER_DEFINED,
                ForL0ReducingState.resolveBuiltinAggType(intSum, TypeAnalyzer.TYPE_INT32));
        }

        @Test
        void aggregatingStateAddDoesNotCallMergeDuringHotPathUpdates() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend();
        try {
            backend.setCurrentKey(9L);
            AggregatingState<Long, Long> state = backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new AggregatingStateDescriptor<>(
                    "agg_no_merge",
                    new NoMergeLongSumAggregateFunction(),
                    LongSerializer.INSTANCE));

            state.add(1L);
            state.add(2L);
            state.add(3L);

            assertEquals(Long.valueOf(6L), state.get());
        } finally {
            backend.dispose();
        }
        }

    // -----------------------------------------------------------------------
    //  Key-set iteration
    // -----------------------------------------------------------------------

    @Test
    void getKeysReflectsRegisteredKeys() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_iter", LongSerializer.INSTANCE));

            for (long k = 0; k < 16; k++) {
                backend.setCurrentKey(k);
                state.update(k * 10);
            }

            Set<Long> expected = new HashSet<>();
            for (long k = 0; k < 16; k++) expected.add(k);
            try (Stream<Long> keys = backend.getKeys("v_iter", VoidNamespace.INSTANCE)) {
                Set<Long> actual = keys.collect(Collectors.toSet());
                assertEquals(expected, actual);
            }

            // After clearing some, they must disappear from getKeys.
            for (long k = 0; k < 8; k++) {
                backend.setCurrentKey(k);
                state.clear();
            }
            Set<Long> afterClear = new HashSet<>();
            for (long k = 8; k < 16; k++) afterClear.add(k);
            try (Stream<Long> keys = backend.getKeys("v_iter", VoidNamespace.INSTANCE)) {
                assertEquals(afterClear, keys.collect(Collectors.toSet()));
            }
        } finally {
            backend.dispose();
        }
    }

    @Test
    void applyToAllKeysVisitsEachKeyExactlyOnce() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            ValueStateDescriptor<Long> desc =
                    new ValueStateDescriptor<>("v_apply", LongSerializer.INSTANCE);
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

            for (long k = 100; k < 110; k++) {
                backend.setCurrentKey(k);
                state.update(k);
            }

            Set<Long> seen = new HashSet<>();
            AtomicInteger callCount = new AtomicInteger();
            backend.applyToAllKeys(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    desc,
                    (KeyedStateFunction<Long, ValueState<Long>>) (key, s) -> {
                        Long v = s.value();
                        // The callback MUST observe the key's stored value
                        // (cache or not), not a stale / empty one.
                        assertEquals(key, v);
                        assertTrue(seen.add(key), "key visited twice: " + key);
                        callCount.incrementAndGet();
                    });
            assertEquals(10, callCount.get());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void numKeyValueStateEntriesMatchesInsertsAndDeletes() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildBackend(true, 4L * 1024 * 1024);
        try {
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_count", LongSerializer.INSTANCE));
            assertEquals(0, backend.numKeyValueStateEntries());

            for (long k = 0; k < 50; k++) {
                backend.setCurrentKey(k);
                state.update(k);
            }
            assertEquals(50, backend.numKeyValueStateEntries());

            for (long k = 0; k < 20; k++) {
                backend.setCurrentKey(k);
                state.clear();
            }
            assertEquals(30, backend.numKeyValueStateEntries());
        } finally {
            backend.dispose();
        }
    }

    private static final class NoMergeLongSumAggregateFunction
            implements AggregateFunction<Long, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Long value, Long accumulator) {
            return accumulator + value;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            throw new AssertionError("merge() must not be used during incremental add() updates");
        }
    }

    @SuppressWarnings("unchecked")
    private static TypeSerializer<RowData> createFixedRowDataSerializer() throws Exception {
        Class<?> rowDataSerializerClass = Class.forName("org.apache.flink.table.runtime.typeutils.RowDataSerializer");
        Constructor<?> ctor = rowDataSerializerClass.getConstructor(LogicalType[].class);
        LogicalType[] fieldTypes = new LogicalType[] {new BigIntType(), new IntType()};
        return (TypeSerializer<RowData>) ctor.newInstance(new Object[] {fieldTypes});
    }

    private static BinaryRowData createFixedRowData(TypeSerializer<RowData> serializer, long left, int right)
            throws Exception {
        Method toBinaryRow = serializer.getClass().getMethod("toBinaryRow", RowData.class);
        return (BinaryRowData) toBinaryRow.invoke(serializer, GenericRowData.of(left, right));
    }
}
