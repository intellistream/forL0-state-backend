package org.apache.flink.state.forl0;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.junit.jupiter.api.Test;

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
}
