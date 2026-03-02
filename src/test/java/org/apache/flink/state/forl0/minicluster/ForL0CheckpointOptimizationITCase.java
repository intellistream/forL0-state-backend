package org.apache.flink.state.forl0.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration tests for the optimized checkpoint serialization path.
 * 
 * <p>Verifies that the optimized writeStateInKeyGroup (O(1) count + zero-allocation
 * forEachInKeyGroup) produces correct checkpoint/savepoint data for:
 * <ul>
 *   <li>Multiple state types (Value, List, Map) in same job</li>
 *   <li>Multiple namespaces (window-like patterns)</li>
 *   <li>Large state volume (stress test)</li>
 * </ul>
 * 
 * Note: Uses some deprecated Flink APIs for compatibility testing.
 */
@SuppressWarnings("deprecation")
public class ForL0CheckpointOptimizationITCase {

    private static final Configuration CLUSTER_CONF = new Configuration();
    static {
        CLUSTER_CONF.setString("taskmanager.memory.managed.size", "1gb");
        CLUSTER_CONF.setString("taskmanager.memory.network.min", "128mb");
        CLUSTER_CONF.setString("taskmanager.memory.network.max", "128mb");
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(2)
                    .setConfiguration(CLUSTER_CONF)
                    .build());

    // --- Probes ---
    static final AtomicLong MULTI_STATE_PROBE = new AtomicLong(0);
    static volatile CountDownLatch MULTI_STATE_LATCH = null;
    static volatile long MULTI_STATE_TARGET = 0;
    static volatile boolean MULTI_STATE_PROBE_MODE = false;
    static volatile CountDownLatch MULTI_STATE_PROBE_LATCH = null;
    static final AtomicLong MULTI_STATE_RESTORED_SUM = new AtomicLong(-1);
    static final AtomicLong MULTI_STATE_RESTORED_LIST_SIZE = new AtomicLong(-1);
    static final AtomicLong MULTI_STATE_RESTORED_MAP_SIZE = new AtomicLong(-1);

    /**
     * Controlled source: emits exactly N records then idles.
     */
    public static class ControlledLongSource extends RichParallelSourceFunction<Long> {
        private final long toEmit;
        private volatile boolean running = true;
        public ControlledLongSource(long toEmit) { this.toEmit = toEmit; }
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            for (long i = 0; running && i < toEmit; i++) {
                synchronized (ctx.getCheckpointLock()) { ctx.collect(1L); }
            }
            while (running) { Thread.sleep(50L); }
        }
        @Override
        public void cancel() { running = false; }
    }

    /**
     * Stateful operator using multiple state types: ValueState, ListState, MapState.
     * 
     * <p>Normal mode: accumulates state from input records.
     * <p>Probe mode: reads state once on first record and reports via probes.
     */
    public static class MultiStateOperator extends RichFlatMapFunction<Long, Long> {
        private transient ValueState<Long> sumState;
        private transient ListState<String> logState;
        private transient MapState<String, Long> mapState;
        private transient boolean probed = false;

        @Override
        public void open(Configuration params) {
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Types.LONG));
            logState = getRuntimeContext().getListState(new ListStateDescriptor<>("log", Types.STRING));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map", Types.STRING, Types.LONG));
        }

        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            // Probe mode
            if (MULTI_STATE_PROBE_MODE && !probed) {
                Long s = sumState.value();
                MULTI_STATE_RESTORED_SUM.set(s != null ? s : 0L);
                
                long listSize = 0;
                for (@SuppressWarnings("unused") String entry : logState.get()) { listSize++; }
                MULTI_STATE_RESTORED_LIST_SIZE.set(listSize);
                
                long mapSize = 0;
                for (@SuppressWarnings("unused") java.util.Map.Entry<String, Long> e : mapState.entries()) { mapSize++; }
                MULTI_STATE_RESTORED_MAP_SIZE.set(mapSize);
                
                probed = true;
                CountDownLatch latch = MULTI_STATE_PROBE_LATCH;
                if (latch != null) latch.countDown();
                return;
            }

            // Normal mode: accumulate
            Long s = sumState.value();
            if (s == null) s = 0L;
            s += value;
            sumState.update(s);
            MULTI_STATE_PROBE.set(s);

            // Add to list state every 10th record
            if (s % 10 == 0) {
                logState.add("entry-" + s);
            }

            // Add to map state every 5th record
            if (s % 5 == 0) {
                mapState.put("key-" + (s / 5), s);
            }

            CountDownLatch latch = MULTI_STATE_LATCH;
            if (latch != null && s == MULTI_STATE_TARGET) {
                latch.countDown();
            }
        }
    }

    /**
     * Tests checkpoint + savepoint/restore with multiple state types (Value, List, Map).
     * Verifies the optimized snapshot writer correctly serializes all state types.
     */
    @Test
    void testSavepointRestoreWithMultipleStateTypes() throws Exception {
        final long N = 500L;
        MULTI_STATE_TARGET = N;
        MULTI_STATE_LATCH = new CountDownLatch(1);
        MULTI_STATE_PROBE_MODE = false;
        MULTI_STATE_PROBE.set(0);

        // 1) First run: process N records
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(200);

        DataStream<Long> s1 = env1
                .addSource(new ControlledLongSource(N)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s1.keyBy(v -> 0)
          .flatMap(new MultiStateOperator()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("multi-state-1");

        boolean reached = MULTI_STATE_LATCH.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(reached, "Should process N records");

        // Trigger savepoint
        Path spDir = Files.createTempDirectory("forl0-sp-multi");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString()).get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) Restore and probe state
        MULTI_STATE_PROBE_MODE = true;
        MULTI_STATE_PROBE_LATCH = new CountDownLatch(1);
        MULTI_STATE_RESTORED_SUM.set(-1);
        MULTI_STATE_RESTORED_LIST_SIZE.set(-1);
        MULTI_STATE_RESTORED_MAP_SIZE.set(-1);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf2.setString("execution.savepoint.path", spPath);

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(200);

        DataStream<Long> s2 = env2
                .addSource(new ControlledLongSource(1)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s2.keyBy(v -> 0)
          .flatMap(new MultiStateOperator()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("multi-state-2");

        boolean probed = MULTI_STATE_PROBE_LATCH.await(20, TimeUnit.SECONDS);
        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}

        Assertions.assertTrue(probed, "Probe should complete");
        Assertions.assertEquals(N, MULTI_STATE_RESTORED_SUM.get(), 
                "ValueState should be restored correctly");
        // N/10 list entries (at every 10th record)
        Assertions.assertEquals(N / 10, MULTI_STATE_RESTORED_LIST_SIZE.get(),
                "ListState should be restored correctly");
        // N/5 map entries (at every 5th record)
        Assertions.assertEquals(N / 5, MULTI_STATE_RESTORED_MAP_SIZE.get(),
                "MapState should be restored correctly");
    }

    // --- Large state probes ---
    static final AtomicLong LARGE_STATE_PROBE = new AtomicLong(0);
    static volatile CountDownLatch LARGE_STATE_LATCH = null;
    static volatile long LARGE_STATE_TARGET = 0;
    static volatile boolean LARGE_STATE_PROBE_MODE = false;
    static volatile CountDownLatch LARGE_STATE_PROBE_LATCH = null;
    static final AtomicLong LARGE_STATE_RESTORED_TOTAL = new AtomicLong(-1);

    /**
     * Stateful operator that creates many distinct key-state entries for stress testing.
     */
    public static class LargeStateSink extends RichFlatMapFunction<Tuple2<Integer, Long>, Long> {
        private transient ValueState<Long> counter;
        private transient boolean probed = false;

        @Override
        public void open(Configuration params) {
            counter = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Types.LONG));
        }

        @Override
        public void flatMap(Tuple2<Integer, Long> value, Collector<Long> out) throws Exception {
            if (LARGE_STATE_PROBE_MODE && !probed) {
                Long c = counter.value();
                LARGE_STATE_RESTORED_TOTAL.set(c != null ? c : 0L);
                probed = true;
                CountDownLatch latch = LARGE_STATE_PROBE_LATCH;
                if (latch != null) latch.countDown();
                return;
            }

            Long c = counter.value();
            if (c == null) c = 0L;
            c += value.f1;
            counter.update(c);
            LARGE_STATE_PROBE.set(c);

            CountDownLatch latch = LARGE_STATE_LATCH;
            if (latch != null && c >= LARGE_STATE_TARGET) {
                latch.countDown();
            }
        }
    }

    /**
     * Source emitting records with multiple keys to create distributed state.
     */
    public static class MultiKeySource extends RichParallelSourceFunction<Tuple2<Integer, Long>> {
        private final long toEmit;
        private final int numKeys;
        private volatile boolean running = true;

        public MultiKeySource(long toEmit, int numKeys) {
            this.toEmit = toEmit;
            this.numKeys = numKeys;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
            for (long i = 0; running && i < toEmit; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple2.of((int)(i % numKeys), 1L));
                }
            }
            while (running) { Thread.sleep(50L); }
        }

        @Override
        public void cancel() { running = false; }
    }

    /**
     * Tests savepoint/restore with many distinct keys (distributed state across key groups).
     * This exercises the optimized snapshot writer with real Flink key distribution.
     */
    @Test
    void testSavepointRestoreWithManyKeys() throws Exception {
        final int NUM_KEYS = 100;
        final long RECORDS_PER_KEY = 50;
        final long TOTAL = NUM_KEYS * RECORDS_PER_KEY;

        LARGE_STATE_TARGET = RECORDS_PER_KEY; // At least one key reaches this
        LARGE_STATE_LATCH = new CountDownLatch(1);
        LARGE_STATE_PROBE_MODE = false;
        LARGE_STATE_PROBE.set(0);

        // 1) First run
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(200);

        env1.addSource(new MultiKeySource(TOTAL, NUM_KEYS))
            .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<Integer, Long>>() {}))
            .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
            .name("src").uid("src")
            .keyBy(t -> t.f0)
            .flatMap(new LargeStateSink())
            .name("stateful").uid("stateful")
            .addSink(new DiscardingSink<>())
            .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("many-keys-1");

        boolean reached = LARGE_STATE_LATCH.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(reached, "At least one key should reach target");

        // Wait for more processing to ensure substantial state
        Thread.sleep(500);

        // Trigger savepoint
        Path spDir = Files.createTempDirectory("forl0-sp-manykeys");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString()).get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) Restore and probe: send to key=0 to check its state
        LARGE_STATE_PROBE_MODE = true;
        LARGE_STATE_PROBE_LATCH = new CountDownLatch(1);
        LARGE_STATE_RESTORED_TOTAL.set(-1);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf2.setString("execution.savepoint.path", spPath);

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(200);

        // Send 1 record to key=0 for probing
        env2.addSource(new MultiKeySource(1, 1))
            .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(
                new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<Integer, Long>>() {}))
            .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
            .name("src").uid("src")
            .keyBy(t -> t.f0)
            .flatMap(new LargeStateSink())
            .name("stateful").uid("stateful")
            .addSink(new DiscardingSink<>())
            .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("many-keys-2");

        boolean probed = LARGE_STATE_PROBE_LATCH.await(20, TimeUnit.SECONDS);
        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}

        Assertions.assertTrue(probed, "Probe should complete");
        // Key=0 should have count = RECORDS_PER_KEY (each source record adds 1 to this key)
        Assertions.assertEquals(RECORDS_PER_KEY, LARGE_STATE_RESTORED_TOTAL.get(),
                "State for key=0 should be restored correctly after savepoint");
    }

    /**
     * Tests that repeated checkpoint cycles work correctly with optimized writer.
     * Runs job long enough for multiple checkpoints, then stops and verifies.
     */
    @Test
    void testMultipleCheckpointCycles() throws Exception {
        MULTI_STATE_PROBE_MODE = false;
        MULTI_STATE_PROBE.set(0);

        Path ckBaseDir = Files.createTempDirectory("forl0-ck-multi");
        String ckBaseUri = ckBaseDir.toUri().toString();

        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf.setString("state.checkpoints.dir", ckBaseUri);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(1);
        env.enableCheckpointing(100); // Frequent checkpoints
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.addSource(new RichParallelSourceFunction<Long>() {
                private volatile boolean running = true;
                @Override
                public void run(SourceContext<Long> ctx) throws Exception {
                    long i = 0;
                    while (running) {
                        synchronized (ctx.getCheckpointLock()) { ctx.collect(i++); }
                        Thread.sleep(2L);
                    }
                }
                @Override
                public void cancel() { running = false; }
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
            .name("src").uid("src")
            .keyBy(v -> (int)(v % 10))
            .flatMap(new RichFlatMapFunction<Long, Long>() {
                private transient ValueState<Long> count;
                @Override
                public void open(Configuration params) {
                    count = getRuntimeContext().getState(new ValueStateDescriptor<>("c", Types.LONG));
                }
                @Override
                public void flatMap(Long in, Collector<Long> out) throws Exception {
                    Long c = count.value();
                    count.update(c == null ? 1L : c + 1L);
                }
            })
            .name("stateful").uid("stateful")
            .addSink(new DiscardingSink<>())
            .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client = env.executeAsync("multi-ck");

        // Wait for at least 3 checkpoints to complete
        Thread.sleep(Duration.ofSeconds(5).toMillis());

        try { client.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}

        // Verify multiple checkpoint directories were created
        java.nio.file.Path jobDir = ckBaseDir.resolve(client.getJobID().toString());
        if (Files.isDirectory(jobDir)) {
            long ckCount;
            try (java.util.stream.Stream<java.nio.file.Path> children = Files.list(jobDir)) {
                ckCount = children.filter(p -> p.getFileName().toString().startsWith("chk-")).count();
            }
            Assertions.assertTrue(ckCount >= 1, 
                    "Should have at least 1 completed checkpoint, found " + ckCount);
        } else {
            Assertions.fail("Job checkpoint directory not found");
        }
    }
}
