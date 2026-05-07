package org.apache.flink.state.forl0;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end behavioral test for the L0 Hot-Key Cache Phase A+B wiring.
 *
 * <p>The test exercises ValueState over the cache-enabled specializations
 * (int64+int64, int64+double, int32+int64, int32+double) and verifies:
 * <ul>
 *   <li>When {@code l0-cache.enabled=false}, semantics are identical to
 *       the no-cache path — this is always true on macOS today.</li>
 *   <li>When {@code l0-cache.enabled=true} and the platform lacks L0
 *       hardware (macOS), the manager gate trips and the cache is forcibly
 *       disabled — manager stats show {@code active=0}, but the
 *       state semantics MUST remain identical (no silent corruption).</li>
 *   <li>Manager metric buffer is populated with 6 slots and is internally
 *       consistent (e.g. {@code freeSets <= totalSets}, no negative values).</li>
 * </ul>
 *
 * <p>Layer-3 hit-rate validation against real L0 hardware is a separate
 * benchmark task and is out of scope for this unit test.
 */
public class HotCacheIntegrationTest {

    private static ForL0KeyedStateBackend<Long> buildLongBackend(
            boolean l0Enabled, long l0Size) throws Exception {
        KeyGroupRange kg = new KeyGroupRange(0, 1);
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

    private static ForL0KeyedStateBackend<Integer> buildIntBackend(
            boolean l0Enabled, long l0Size) throws Exception {
        KeyGroupRange kg = new KeyGroupRange(0, 1);
        ForL0KeyedStateBackendBuilder<Integer> b = new ForL0KeyedStateBackendBuilder<>(
                null,
                IntSerializer.INSTANCE,
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
    //  Correctness: <long, long> ValueState round-trip with cache disabled
    //  and (attempted) cache enabled — results must be identical.
    // -----------------------------------------------------------------------
    @Test
    void longLongValueStateBehavesIdenticallyWithCacheOnOrOff() throws Exception {
        for (boolean enabled : new boolean[] {false, true}) {
            ForL0KeyedStateBackend<Long> backend = buildLongBackend(enabled, 8L * 1024 * 1024);
            try {
                backend.setCurrentKey(7L);
                ValueState<Long> state = backend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("v_long_long", LongSerializer.INSTANCE));

                assertNull(state.value(), "initial value should be null (enabled=" + enabled + ")");
                state.update(42L);
                assertEquals(Long.valueOf(42L), state.value());
                state.update(100L); // update in place
                assertEquals(Long.valueOf(100L), state.value());
                state.clear();
                assertNull(state.value(), "clear must drop cached entry too");
            } finally {
                backend.dispose();
            }
        }
    }

    @Test
    void longDoubleValueStateRoundtrip() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildLongBackend(true, 8L * 1024 * 1024);
        try {
            backend.setCurrentKey(9L);
            ValueState<Double> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_long_double", DoubleSerializer.INSTANCE));

            assertNull(state.value());
            state.update(3.14159);
            assertEquals(Double.valueOf(3.14159), state.value());
            // Check a tricky IEEE-754 pattern round-trips via the bit-cast cache path.
            state.update(Double.NEGATIVE_INFINITY);
            assertEquals(Double.valueOf(Double.NEGATIVE_INFINITY), state.value());
            state.clear();
            assertNull(state.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void intLongValueStateRoundtrip() throws Exception {
        ForL0KeyedStateBackend<Integer> backend = buildIntBackend(true, 8L * 1024 * 1024);
        try {
            backend.setCurrentKey(123);
            ValueState<Long> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_int_long", LongSerializer.INSTANCE));

            assertNull(state.value());
            state.update(Long.MAX_VALUE);
            assertEquals(Long.valueOf(Long.MAX_VALUE), state.value());
            state.update(-1L);
            assertEquals(Long.valueOf(-1L), state.value());
            state.clear();
            assertNull(state.value());
        } finally {
            backend.dispose();
        }
    }

    @Test
    void intDoubleValueStateRoundtrip() throws Exception {
        ForL0KeyedStateBackend<Integer> backend = buildIntBackend(true, 8L * 1024 * 1024);
        try {
            backend.setCurrentKey(-42);
            ValueState<Double> state = backend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("v_int_double", DoubleSerializer.INSTANCE));

            assertNull(state.value());
            state.update(2.718281828);
            assertEquals(Double.valueOf(2.718281828), state.value());
            state.clear();
            assertNull(state.value());
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  Hardware gating: on macOS the cache MUST not actually activate even
    //  when enabled=true. The manager reports active=0 in that case.
    // -----------------------------------------------------------------------
    @Test
    void managerGateClosesOnPlatformsWithoutL0() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildLongBackend(true, 4L * 1024 * 1024);
        try {
            long engineHandle = backend.getEngineHandle();
            long[] mgrStats = new long[6];
            NativeEngine.getHotCacheManagerStats(engineHandle, mgrStats);

            // On macOS / any box without /dev/hisi_l0, active must be 0 and the
            // capacity must be reported as 0 so downstream metrics don't lie.
            String os = System.getProperty("os.name", "").toLowerCase();
            if (os.contains("mac") || os.contains("darwin")) {
                assertEquals(0L, mgrStats[0], "manager must NOT activate on macOS");
                assertEquals(0L, mgrStats[1], "capacity_bytes must be 0 when gate trips");
                assertEquals(0L, mgrStats[3], "total_sets must be 0 when gate trips");
            }
            // Cross-platform invariants.
            assertTrue(mgrStats[4] <= mgrStats[3], "freeSets must be <= totalSets");
            assertTrue(mgrStats[2] <= mgrStats[1], "usedBytes must be <= capacityBytes");
            for (long v : mgrStats) {
                assertTrue(v >= 0, "no metric slot may be negative");
            }
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  When the cache is disabled by config the manager metric buffer stays
    //  all-zero and per-state cache attachment is absent.
    // -----------------------------------------------------------------------
    @Test
    void managerMetricsAreZeroWhenDisabledByConfig() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildLongBackend(false, 0L);
        try {
            long engineHandle = backend.getEngineHandle();
            long[] mgrStats = new long[6];
            NativeEngine.getHotCacheManagerStats(engineHandle, mgrStats);
            for (int i = 0; i < mgrStats.length; i++) {
                assertEquals(0L, mgrStats[i], "slot " + i + " must be 0 when disabled");
            }
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  Extended manager metric buffer (9 slots) exposes the Phase C
    //  aggregate counters. The JNI is forward-compatible: a 6-slot caller
    //  still works, a 9-slot caller gets 3 extra counters populated.
    // -----------------------------------------------------------------------
    @Test
    void extendedManagerMetricsExposeAggregateCounters() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildLongBackend(false, 0L);
        try {
            long engineHandle = backend.getEngineHandle();
            long[] mgrStats = new long[9];
            NativeEngine.getHotCacheManagerStats(engineHandle, mgrStats);
            for (int i = 0; i < mgrStats.length; i++) {
                assertEquals(0L, mgrStats[i], "slot " + i + " must be 0 when disabled");
            }
        } finally {
            backend.dispose();
        }
    }

    // -----------------------------------------------------------------------
    //  `NativeEngine.rebalanceHotCache` must be safe to invoke on a backend
    //  whose manager is inactive (hardware unavailable or disabled). It
    //  returns 0 without doing anything and without crashing the JVM.
    // -----------------------------------------------------------------------
    @Test
    void rebalanceIsSafeWhenManagerInactive() throws Exception {
        ForL0KeyedStateBackend<Long> backend = buildLongBackend(false, 0L);
        try {
            long engineHandle = backend.getEngineHandle();
            int rebalanced = NativeEngine.rebalanceHotCache(
                    engineHandle, /*intervalOps=*/ 1L << 20, /*missRateThreshold=*/ 0.5);
            assertEquals(0, rebalanced);
        } finally {
            backend.dispose();
        }
    }
}
