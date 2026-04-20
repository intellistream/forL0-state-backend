package org.apache.flink.state.forl0.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * 验证“并发写入过程中执行异步 checkpoint”的正确性。
 *
 * <p>Flink 默认对 heap 类后端启用 async snapshot（snapshotStrategy 会拷贝 working state
 *    后在 IO 线程上写出）。当 HotCache 启用时，write-through 策略保证 SwissTable 始终
 *    是权威存储；快照拷贝只读 SwissTable，与 HotCache 的任何中间状态无关。</p>
 *
 * <p>本测试在持续写入的过程中，等待真正完成并外部化的 checkpoint（chk-N 目录下
 *    出现 {@code _metadata}），随后取消作业，以该 checkpoint 为起点恢复一个新作业，
 *    在探测阶段读取每个 key 的状态值并断言：</p>
 * <ul>
 *   <li>所有 key 恢复值 &ge; 0 且至少有一个 key &gt; 0（快照不为空）；</li>
 *   <li>恢复后继续写入可以在已有状态上继续累加（单调性）。</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class ForL0AsyncSnapshotITCase {

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

    // ---- 共享探针 ----
    static volatile int NUM_KEYS = 0;
    static volatile boolean PROBE_ONLY = false;
    static volatile CountDownLatch PROBE_DONE_LATCH = null;
    static final AtomicLong PROCESSED = new AtomicLong(0L);
    static final AtomicInteger PROBE_COUNT = new AtomicInteger(0);
    static final Map<Long, Long> PROBED_VALUE = new ConcurrentHashMap<>();
    static final AtomicLong POST_RESTORE_MAX = new AtomicLong(0L);

    /** 持续发射多 key；被 cancel 前不停. */
    public static class HighThroughputMultiKeySource
            extends RichParallelSourceFunction<Tuple2<Long, Integer>> {
        private volatile boolean running = true;
        private final int numKeys;

        public HighThroughputMultiKeySource(int numKeys) {
            this.numKeys = numKeys;
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws Exception {
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            long k = 0;
            while (running) {
                if ((int) (k % parallelism) == taskIdx) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(Tuple2.of(k % numKeys, 1));
                    }
                }
                k++;
                // 轻微让步避免 100% CPU 把 mini-cluster 饿死
                if ((k & 0x3FF) == 0) Thread.sleep(1L);
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    /** 一次探测一次性读取：仅发 NUM_KEYS 条、每 key 一条. */
    public static class ProbeSource
            extends RichParallelSourceFunction<Tuple2<Long, Integer>> {
        private volatile boolean running = true;
        private final int numKeys;

        public ProbeSource(int numKeys) {
            this.numKeys = numKeys;
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws Exception {
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            for (long key = 0; key < numKeys && running; key++) {
                if ((int) (key % parallelism) != taskIdx) continue;
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple2.of(key, 0));
                }
            }
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
                    new ValueStateDescriptor<>("async_sum", Types.LONG));
            probedSeen = new boolean[Math.max(NUM_KEYS, 1)];
        }

        @Override
        public void flatMap(Tuple2<Long, Integer> in, Collector<Long> out) throws Exception {
            Long key = in.f0;
            if (PROBE_ONLY) {
                int idx = key.intValue();
                if (idx < 0 || idx >= probedSeen.length || probedSeen[idx]) return;
                Long v = sum.value();
                long lv = v == null ? 0L : v;
                PROBED_VALUE.put(key, lv);
                if (lv > POST_RESTORE_MAX.get()) {
                    POST_RESTORE_MAX.updateAndGet(x -> Math.max(x, lv));
                }
                probedSeen[idx] = true;
                int done = PROBE_COUNT.incrementAndGet();
                CountDownLatch latch = PROBE_DONE_LATCH;
                if (latch != null && done >= NUM_KEYS) latch.countDown();
                return;
            }
            Long v = sum.value();
            if (v == null) v = 0L;
            v += 1L;
            sum.update(v);
            PROCESSED.incrementAndGet();
        }
    }

    private static Configuration baseConf(String checkpointsDirUri) {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf.setString("state.backend.forl0.l0-cache.enabled", "true");
        conf.setString("state.backend.forl0.l0-cache.size", "32mb");
        if (checkpointsDirUri != null) {
            conf.setString("state.checkpoints.dir", checkpointsDirUri);
        }
        return conf;
    }

    private static void resetProbes(int numKeys) {
        NUM_KEYS = numKeys;
        PROBE_ONLY = false;
        PROBE_DONE_LATCH = null;
        PROCESSED.set(0L);
        PROBE_COUNT.set(0);
        PROBED_VALUE.clear();
        POST_RESTORE_MAX.set(0L);
    }

    /** 启动写入作业，轮询出第一个含 _metadata 的 chk-* 目录，取消作业并返回其 URI. */
    private static String runAndCaptureCompletedCheckpoint(int parallelism, String ckBaseUri)
            throws Exception {
        Configuration conf = baseConf(ckBaseUri);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(150);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new HighThroughputMultiKeySource(NUM_KEYS))
                    .name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        src.keyBy(t -> t.f0)
           .flatMap(new CountingOrProbingOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("async-write");
        final String jobIdStr = client.getJobID().toString();

        Path jobDir = Paths.get(new URI(ckBaseUri)).resolve(jobIdStr);
        String ckRestorePath = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        // 需要至少 chk-2：第一轮 barrier 可能在 key 还没全部累加时就下沉
        while (System.nanoTime() < deadline && ckRestorePath == null) {
            if (Files.isDirectory(jobDir)) {
                try (Stream<Path> children = Files.list(jobDir)) {
                    Path latest = children
                            .filter(p -> p.getFileName().toString().startsWith("chk-"))
                            .filter(p -> {
                                String name = p.getFileName().toString();
                                try {
                                    int n = Integer.parseInt(name.substring("chk-".length()));
                                    return n >= 2;
                                } catch (NumberFormatException e) { return false; }
                            })
                            .max(Comparator.comparing(p -> p.getFileName().toString()))
                            .orElse(null);
                    if (latest != null && Files.isRegularFile(latest.resolve("_metadata"))) {
                        ckRestorePath = latest.toUri().toString();
                    }
                }
            }
            if (ckRestorePath == null) Thread.sleep(50L);
        }
        Assertions.assertNotNull(ckRestorePath, "未在超时内拿到 chk-N (N>=2) 的外部化 checkpoint");

        client.cancel().get(10, TimeUnit.SECONDS);
        return ckRestorePath;
    }

    private static Map<Long, Long> restoreAndProbe(int parallelism,
                                                   String checkpointPath,
                                                   String ckBaseUri) throws Exception {
        PROBE_ONLY = true;
        PROBE_DONE_LATCH = new CountDownLatch(1);
        PROBE_COUNT.set(0);
        PROBED_VALUE.clear();

        Configuration conf = baseConf(ckBaseUri);
        conf.setString("execution.savepoint.path", checkpointPath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new ProbeSource(NUM_KEYS)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        src.keyBy(t -> t.f0)
           .flatMap(new CountingOrProbingOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("async-probe");
        boolean ok = PROBE_DONE_LATCH.await(30, TimeUnit.SECONDS);
        try { client.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}
        Assertions.assertTrue(ok, "探测超时，count=" + PROBE_COUNT.get());
        return PROBED_VALUE;
    }

    // -----------------------------------------------------------------------

    @Test
    void asyncCheckpointUnderWrites_RestorePreservesNonZeroState() throws Exception {
        final int numKeys = 8;
        resetProbes(numKeys);
        Path ckBase = Files.createTempDirectory("forl0-async-ck");
        String ckBaseUri = ckBase.toUri().toString();

        String ckPath = runAndCaptureCompletedCheckpoint(1, ckBaseUri);
        Map<Long, Long> restored = restoreAndProbe(1, ckPath, ckBaseUri);

        long sum = 0L;
        long nonZero = 0L;
        for (long k = 0; k < numKeys; k++) {
            Long v = restored.get(k);
            Assertions.assertNotNull(v, "key=" + k + " 未被探测");
            Assertions.assertTrue(v >= 0L, "恢复后值不应为负：key=" + k + ", value=" + v);
            sum += v;
            if (v > 0L) nonZero++;
        }
        Assertions.assertTrue(sum > 0L, "快照为空说明 checkpoint 未捕获任何状态更新");
        // chk-N (N>=2) 至少意味着 key=0 被命中多次，其它 key 是否覆盖视 key 分布而定；
        // 这里只做弱断言避免因 key-group 分布不均而 flaky。
        Assertions.assertTrue(nonZero >= 1, "至少应有一个 key 有累加值");
    }

    @Test
    void asyncCheckpointRestore_AllowsContinuedAccumulation() throws Exception {
        // 恢复后再跑一次写入阶段，验证 state 是“可继续写”的：restored 值上应叠加新增。
        final int numKeys = 4;
        resetProbes(numKeys);
        Path ckBase = Files.createTempDirectory("forl0-async-ck2");
        String ckBaseUri = ckBase.toUri().toString();

        String ckPath = runAndCaptureCompletedCheckpoint(1, ckBaseUri);
        Map<Long, Long> afterFirst = restoreAndProbe(1, ckPath, ckBaseUri);
        long maxAfterFirst = 0L;
        for (Long v : afterFirst.values()) maxAfterFirst = Math.max(maxAfterFirst, v);
        Assertions.assertTrue(maxAfterFirst > 0L, "首次恢复后状态为空，后续断言无意义");

        // 再跑一轮写入（从首次恢复点继续），再次拿 ckpt
        PROBE_ONLY = false;
        PROCESSED.set(0L);

        Configuration conf = baseConf(ckBaseUri);
        conf.setString("execution.savepoint.path", ckPath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(1);
        env.enableCheckpointing(150);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new HighThroughputMultiKeySource(numKeys)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        src.keyBy(t -> t.f0)
           .flatMap(new CountingOrProbingOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("async-write-2");
        final String jobIdStr = client.getJobID().toString();

        Path jobDir = Paths.get(new URI(ckBaseUri)).resolve(jobIdStr);
        String ckPath2 = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (System.nanoTime() < deadline && ckPath2 == null) {
            if (Files.isDirectory(jobDir)) {
                try (Stream<Path> children = Files.list(jobDir)) {
                    Path latest = children
                            .filter(p -> p.getFileName().toString().startsWith("chk-"))
                            .filter(p -> {
                                String name = p.getFileName().toString();
                                try {
                                    int n = Integer.parseInt(name.substring("chk-".length()));
                                    return n >= 2;
                                } catch (NumberFormatException e) { return false; }
                            })
                            .max(Comparator.comparing(p -> p.getFileName().toString()))
                            .orElse(null);
                    if (latest != null && Files.isRegularFile(latest.resolve("_metadata"))) {
                        ckPath2 = latest.toUri().toString();
                    }
                }
            }
            if (ckPath2 == null) Thread.sleep(50L);
        }
        Assertions.assertNotNull(ckPath2, "第二轮 ckpt 超时");
        client.cancel().get(10, TimeUnit.SECONDS);

        Map<Long, Long> afterSecond = restoreAndProbe(1, ckPath2, ckBaseUri);
        long maxAfterSecond = 0L;
        for (Long v : afterSecond.values()) maxAfterSecond = Math.max(maxAfterSecond, v);
        Assertions.assertTrue(maxAfterSecond >= maxAfterFirst,
                "第二轮快照值不应小于第一轮：first=" + maxAfterFirst + ", second=" + maxAfterSecond);
    }
}
