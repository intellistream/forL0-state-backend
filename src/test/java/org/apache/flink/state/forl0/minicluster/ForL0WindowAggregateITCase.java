package org.apache.flink.state.forl0.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration test for ForL0StateBackend with sliding window aggregation.
 * 
 * <p>This test reproduces the exact scenario from WordCount benchmark:
 * - Sliding event-time windows
 * - AggregateFunction with Tuple2<Long, Long> accumulator (16 bytes, exceeds INLINE_THRESHOLD)
 * - ProcessWindowFunction for result extraction
 * 
 * <p>The test validates that the zero-copy fast path for Tuple types works correctly
 * in the context of window aggregation where AggregatingState.add() internally calls
 * StateMap.transform().
 */
@SuppressWarnings("deprecation")
public class ForL0WindowAggregateITCase {

    private static final int PARALLELISM = 2;

    private static final Configuration CLUSTER_CONF = new Configuration();
    static {
        CLUSTER_CONF.setString("taskmanager.memory.managed.size", "512mb");
        CLUSTER_CONF.setString("taskmanager.memory.network.min", "64mb");
        CLUSTER_CONF.setString("taskmanager.memory.network.max", "64mb");
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(PARALLELISM + 1)
                    .setConfiguration(CLUSTER_CONF)
                    .build());

    /**
     * Test sliding window aggregation with Tuple2<Long, Long> accumulator.
     * This is the exact pattern used in WordCount benchmark that triggered NPE.
     */
    @Test
    void testSlidingWindowAggregateWithTuple2LongLong() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(PARALLELISM);

        // Generate test data: (word, timestamp)
        // Use bounded source with explicit timestamps for deterministic testing
        List<Tuple2<String, Long>> inputData = new ArrayList<>();
        long baseTime = 1000L;
        for (int i = 0; i < 100; i++) {
            String word = "word" + (i % 10);  // 10 distinct words
            long timestamp = baseTime + i * 100;  // 100ms apart
            inputData.add(Tuple2.of(word, timestamp));
        }

        DataStream<Tuple2<String, Long>> source = env
                .fromCollection(inputData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                                .withTimestampAssigner((event, ts) -> event.f1)
                );

        // Sliding window aggregation - exact same pattern as WordCount benchmark
        DataStream<Tuple3<String, Long, Long>> result = source
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(
                        Duration.ofMillis(1000),   // window size
                        Duration.ofMillis(200)     // slide size
                ))
                .aggregate(
                        new CountAggregator(),
                        new WindowResultFunction()
                );

        // Collect results
        List<Tuple3<String, Long, Long>> results = new ArrayList<>();
        try (CloseableIterator<Tuple3<String, Long, Long>> it = result.executeAndCollect()) {
            while (it.hasNext()) {
                results.add(it.next());
            }
        }

        // Verify results - should have multiple window outputs
        Assertions.assertFalse(results.isEmpty(), "Should have window results");
        
        // Verify that each result has valid data
        for (Tuple3<String, Long, Long> r : results) {
            Assertions.assertNotNull(r.f0, "Word should not be null");
            Assertions.assertTrue(r.f0.startsWith("word"), "Word should start with 'word'");
            Assertions.assertTrue(r.f1 > 0, "Count should be positive");
            Assertions.assertTrue(r.f2 > 0, "Timestamp should be positive");
        }

        System.out.println("Window aggregation test passed with " + results.size() + " window outputs");
    }

    /**
     * Test with higher load to stress the state backend.
     */
    @Test
    void testHighLoadWindowAggregate() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(PARALLELISM);

        // Generate more test data
        final int numRecords = 10000;
        final int numKeys = 1000;
        
        List<Tuple2<String, Long>> inputData = new ArrayList<>();
        long baseTime = 1000L;
        for (int i = 0; i < numRecords; i++) {
            String word = "key" + (i % numKeys);
            long timestamp = baseTime + i * 10;  // 10ms apart
            inputData.add(Tuple2.of(word, timestamp));
        }

        DataStream<Tuple2<String, Long>> source = env
                .fromCollection(inputData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                                .withTimestampAssigner((event, ts) -> event.f1)
                );

        DataStream<Tuple3<String, Long, Long>> result = source
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(
                        Duration.ofMillis(5000),
                        Duration.ofMillis(1000)
                ))
                .aggregate(
                        new CountAggregator(),
                        new WindowResultFunction()
                );

        // Collect and verify results
        List<Tuple3<String, Long, Long>> results = new ArrayList<>();
        try (CloseableIterator<Tuple3<String, Long, Long>> it = result.executeAndCollect()) {
            while (it.hasNext()) {
                results.add(it.next());
            }
        }

        Assertions.assertFalse(results.isEmpty(), "Should have window results");
        
        // Count total records processed
        long totalCount = results.stream().mapToLong(r -> r.f1).sum();
        System.out.println("High load test: " + results.size() + " windows, total count: " + totalCount);
    }

    // --- Window + checkpoint/restore probes ---
    static final AtomicLong WINDOW_SUM_PROBE = new AtomicLong(0);

    /**
     * Unbounded source emitting (key, eventTimestamp) tuples with steadily advancing time.
     */
    public static class UnboundedEventTimeSource extends RichParallelSourceFunction<Tuple2<String, Long>> {
        private volatile boolean running = true;
        private final int numKeys;
        private final long intervalMs;

        public UnboundedEventTimeSource(int numKeys, long intervalMs) {
            this.numKeys = numKeys;
            this.intervalMs = intervalMs;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            long ts = 1000L;
            long i = 0;
            while (running) {
                String key = "key" + (i % numKeys);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple2.of(key, ts));
                }
                if (i % numKeys == numKeys - 1) {
                    ts += intervalMs;
                }
                i++;
                Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    /**
     * Sink that tracks total aggregated window output via probe.
     */
    public static class WindowOutputSink extends RichFlatMapFunction<Tuple3<String, Long, Long>, Long> {
        @Override
        public void flatMap(Tuple3<String, Long, Long> value, Collector<Long> out) throws Exception {
            WINDOW_SUM_PROBE.addAndGet(value.f1);
        }
    }

    /**
     * Tests window aggregation with checkpoint/restore to verify TimeWindow namespace + COW.
     */
    @Test
    void testWindowAggregateCheckpointRestore() throws Exception {
        WINDOW_SUM_PROBE.set(0);

        // 1) First run with windowed aggregation
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(500);
        env1.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env1.addSource(new UnboundedEventTimeSource(5, 100))
            .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Long>>() {})
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(50))
                    .withTimestampAssigner((event, ts) -> event.f1))
            .name("src").uid("src")
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Duration.ofMillis(500)))
            .aggregate(new CountAggregator(), new WindowResultFunction())
            .name("window").uid("window")
            .flatMap(new WindowOutputSink())
            .name("sink").uid("sink")
            .addSink(new DiscardingSink<>())
            .name("discard").uid("discard");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("window-ckpt-1");

        // Wait for some window output (generous timeout)
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && WINDOW_SUM_PROBE.get() < 5) {
            Thread.sleep(100);
        }
        long sumBeforeSavepoint = WINDOW_SUM_PROBE.get();
        Assertions.assertTrue(sumBeforeSavepoint > 0, "Should have produced window output before savepoint");

        // Trigger savepoint
        Path spDir = Files.createTempDirectory("forl0-sp-window");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString()).get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) Restore from savepoint
        WINDOW_SUM_PROBE.set(0);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf2.setString("execution.savepoint.path", spPath);
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(500);
        env2.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env2.addSource(new UnboundedEventTimeSource(5, 100))
            .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Long>>() {})
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofMillis(50))
                    .withTimestampAssigner((event, ts) -> event.f1))
            .name("src").uid("src")
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Duration.ofMillis(500)))
            .aggregate(new CountAggregator(), new WindowResultFunction())
            .name("window").uid("window")
            .flatMap(new WindowOutputSink())
            .name("sink").uid("sink")
            .addSink(new DiscardingSink<>())
            .name("discard").uid("discard");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("window-ckpt-2");

        // Wait for continued output after restore
        long deadline2 = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline2 && WINDOW_SUM_PROBE.get() < 5) {
            Thread.sleep(100);
        }

        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}

        long sumAfterRestore = WINDOW_SUM_PROBE.get();
        Assertions.assertTrue(sumAfterRestore > 0,
                "Window aggregation should continue producing output after restore, got " + sumAfterRestore);
    }

    /**
     * Aggregator that counts occurrences and tracks the latest record timestamp.
     * Accumulator: Tuple2<Long, Long> = (count, latestTimestamp) = 16 bytes
     * 
     * This is the exact same aggregator used in WordCount benchmark.
     * The 16-byte Tuple2<Long, Long> exceeds INLINE_THRESHOLD (8 bytes),
     * triggering the pointer mode in EntryStore.
     */
    public static class CountAggregator 
            implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }
        
        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
            return Tuple2.of(accumulator.f0 + 1, Math.max(accumulator.f1, value.f1));
        }
        
        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
            return accumulator;
        }
        
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, Math.max(a.f1, b.f1));
        }
    }
    
    /**
     * Window function that produces (word, count, sourceTimestamp) tuples.
     */
    public static class WindowResultFunction 
            extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple3<String, Long, Long>, String, TimeWindow> {
        
        @Override
        public void process(String key,
                          Context context,
                          Iterable<Tuple2<Long, Long>> results,
                          Collector<Tuple3<String, Long, Long>> out) {
            Tuple2<Long, Long> result = results.iterator().next();
            out.collect(Tuple3.of(key, result.f0, result.f1));
        }
    }
}
