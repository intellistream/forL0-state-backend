package org.apache.flink.state.forl0.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 通过 savepoint 改变并行度（rescale）的集成测试。
 *
 * <p>覆盖两个场景：</p>
 * <ul>
 *   <li>Scale-up：并行度 1 → 2，验证 key-group 重分配后每个 key 的状态完好。</li>
 *   <li>Scale-down：并行度 2 → 1，验证多个 subtask 的状态合并到单个 subtask 后依然正确。</li>
 * </ul>
 *
 * <p>HotCache 在 rescale 时会走 {@code release_ll} / {@code acquire_ll} 路径，
 *    本测试用于兜底这条路径的正确性；macOS 下硬件网关关闭、cache 无效，结果应与无 cache 完全一致。</p>
 */
@SuppressWarnings("deprecation")
public class ForL0RescaleITCase {

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
                    .setNumberSlotsPerTaskManager(4)   // rescale 至少需要 4 slot
                    .setConfiguration(CLUSTER_CONF)
                    .build());

    // --- 跨 JVM 线程共享的探针：所有字段必须 static ---
    static volatile int NUM_KEYS = 0;
    static volatile long ROUNDS = 0L;
    static volatile boolean PROBE_ONLY = false;
    /** 写入阶段：当算子累计处理到 NUM_KEYS * ROUNDS 条时 countDown. */
    static volatile CountDownLatch WRITE_DONE_LATCH = null;
    /** 读取阶段：当算子已为每个 key 读取过一次时 countDown. */
    static volatile CountDownLatch PROBE_DONE_LATCH = null;
    static final AtomicLong PROCESSED_COUNT = new AtomicLong(0L);
    static final AtomicInteger PROBE_COUNT = new AtomicInteger(0);
    /** 读取阶段记录每个 key 恢复后的值；key 唯一故线程安全。 */
    static final Map<Long, Long> PROBED_VALUES = new ConcurrentHashMap<>();

    /** 写模式发射：[(0,_), (1,_), ..., (N-1,_)] × R。读模式发射：[(0,_)..(N-1,_)] 一轮. */
    public static class MultiKeySource extends RichParallelSourceFunction<Tuple2<Long, Integer>> {
        private volatile boolean running = true;
        private final int numKeys;
        private final long rounds;

        public MultiKeySource(int numKeys, long rounds) {
            this.numKeys = numKeys;
            this.rounds = rounds;
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws Exception {
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            long r = 0;
            while (running && r < rounds) {
                for (long key = 0; key < numKeys && running; key++) {
                    // 任务间均分 key，避免重复发射
                    if ((int) (key % parallelism) != taskIdx) continue;
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(Tuple2.of(key, 1));
                    }
                }
                r++;
            }
            // 保持作业 RUNNING 以便外部触发 savepoint
            while (running) { Thread.sleep(20L); }
        }

        @Override
        public void cancel() { running = false; }
    }

    public static class CountingOrProbingOp
            extends RichFlatMapFunction<Tuple2<Long, Integer>, Long> {
        private transient ValueState<Long> sum;
        private transient boolean[] probedSeen;

        @Override
        public void open(Configuration parameters) {
            sum = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("rescale_sum", Types.LONG));
            probedSeen = new boolean[Math.max(NUM_KEYS, 1)];
        }

        @Override
        public void flatMap(Tuple2<Long, Integer> in, Collector<Long> out) throws Exception {
            Long key = in.f0;
            if (PROBE_ONLY) {
                int idx = key.intValue();
                if (idx < 0 || idx >= probedSeen.length || probedSeen[idx]) {
                    return;
                }
                Long v = sum.value();
                PROBED_VALUES.put(key, v == null ? 0L : v);
                probedSeen[idx] = true;
                int done = PROBE_COUNT.incrementAndGet();
                CountDownLatch latch = PROBE_DONE_LATCH;
                if (latch != null && done >= NUM_KEYS) {
                    latch.countDown();
                }
                return;
            }
            Long v = sum.value();
            if (v == null) v = 0L;
            v += 1L;
            sum.update(v);
            long total = PROCESSED_COUNT.incrementAndGet();
            CountDownLatch latch = WRITE_DONE_LATCH;
            if (latch != null && total >= (long) NUM_KEYS * ROUNDS) {
                latch.countDown();
            }
        }
    }

    private static void resetProbes(int numKeys, long rounds) {
        NUM_KEYS = numKeys;
        ROUNDS = rounds;
        PROBE_ONLY = false;
        WRITE_DONE_LATCH = new CountDownLatch(1);
        PROBE_DONE_LATCH = null;
        PROCESSED_COUNT.set(0L);
        PROBE_COUNT.set(0);
        PROBED_VALUES.clear();
    }

    private static String runUntilSavepoint(int parallelism, String savepointDirUri) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new MultiKeySource(NUM_KEYS, ROUNDS)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        src.keyBy(t -> t.f0)
           .flatMap(new CountingOrProbingOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("rescale-write-" + parallelism);

        boolean reached = WRITE_DONE_LATCH.await(40, TimeUnit.SECONDS);
        Assertions.assertTrue(reached,
                "写阶段未达到目标: processed=" + PROCESSED_COUNT.get()
                        + ", expected=" + (long) NUM_KEYS * ROUNDS);

        String spPath = client.triggerSavepoint(savepointDirUri).get(40, TimeUnit.SECONDS);
        client.cancel().get(10, TimeUnit.SECONDS);
        return spPath;
    }

    private static void restoreAndProbe(int parallelism, String savepointPath) throws Exception {
        PROBE_ONLY = true;
        PROBE_DONE_LATCH = new CountDownLatch(1);
        PROBE_COUNT.set(0);
        PROBED_VALUES.clear();

        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf.setString("execution.savepoint.path", savepointPath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new MultiKeySource(NUM_KEYS, 1L)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        src.keyBy(t -> t.f0)
           .flatMap(new CountingOrProbingOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("rescale-probe-" + parallelism);

        boolean probed = PROBE_DONE_LATCH.await(40, TimeUnit.SECONDS);
        try { client.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}
        Assertions.assertTrue(probed,
                "探测阶段未覆盖所有 key: count=" + PROBE_COUNT.get() + ", expected=" + NUM_KEYS);
    }

    private static void assertEachKeyEqualsRounds(long expectedPerKey) {
        for (long k = 0; k < NUM_KEYS; k++) {
            Long v = PROBED_VALUES.get(k);
            Assertions.assertNotNull(v, "key=" + k + " 未被探测到");
            Assertions.assertEquals(expectedPerKey, v.longValue(),
                    "key=" + k + " 恢复后的值不符合预期");
        }
    }

    // -----------------------------------------------------------------------

    @Test
    void testScaleUp_1_to_2() throws Exception {
        final int numKeys = 8;
        final long rounds = 500L;
        resetProbes(numKeys, rounds);
        Path spDir = Files.createTempDirectory("forl0-sp-scaleup");

        String spPath = runUntilSavepoint(1, spDir.toUri().toString());
        restoreAndProbe(2, spPath);
        assertEachKeyEqualsRounds(rounds);
    }

    @Test
    void testScaleDown_2_to_1() throws Exception {
        final int numKeys = 8;
        final long rounds = 500L;
        resetProbes(numKeys, rounds);
        Path spDir = Files.createTempDirectory("forl0-sp-scaledown");

        String spPath = runUntilSavepoint(2, spDir.toUri().toString());
        restoreAndProbe(1, spPath);
        assertEachKeyEqualsRounds(rounds);
    }

    @Test
    void testScaleUp_2_to_4() throws Exception {
        final int numKeys = 16;
        final long rounds = 250L;
        resetProbes(numKeys, rounds);
        Path spDir = Files.createTempDirectory("forl0-sp-scaleup24");

        String spPath = runUntilSavepoint(2, spDir.toUri().toString());
        restoreAndProbe(4, spPath);
        assertEachKeyEqualsRounds(rounds);
    }
}
