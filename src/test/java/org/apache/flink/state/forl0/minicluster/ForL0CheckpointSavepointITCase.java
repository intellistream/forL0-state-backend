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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * 针对 ForL0StateBackend 的 checkpoint 与 savepoint/restore 功能测试。
 * Note: Uses some deprecated Flink APIs for compatibility testing.
 */
@SuppressWarnings("deprecation")
public class ForL0CheckpointSavepointITCase {

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

    // --- 静态探针 ---
    static final AtomicInteger CHECKPOINT_COMPLETED = new AtomicInteger();
    static final AtomicLong STATE_SUM_PROBE = new AtomicLong();

    /**
     * 无限递增源，用于保持作业运行，便于产生 checkpoint/savepoint。
     */
    public static class InfiniteLongSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long i = 0L;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(i++);
                }
                Thread.sleep(5L);
            }
        }
        @Override
        public void cancel() { running = false; }
    }

    /**
     * 有状态算子：对固定 key 累加 1。
     */
    public static class StatefulCounter extends RichFlatMapFunction<Long, Long> {
        private transient ValueState<Long> sum;
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> d = new ValueStateDescriptor<>("sum", Types.LONG);
            sum = getRuntimeContext().getState(d);
        }
        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            Long s = sum.value();
            if (s == null) s = 0L;
            s += 1L;
            sum.update(s);
            // 更新探针，便于测试线程观测到进展
            STATE_SUM_PROBE.set(s);
        }
    }

    /**
     * 可控源：精确发送 toEmit 条数据后停止发送并保持运行，便于触发 savepoint；可用于“探测”时仅发 1 条。
     */
    public static class ControlledSource extends RichParallelSourceFunction<Long> {
        private final long toEmit;
        private volatile boolean running = true;
        public ControlledSource(long toEmit) { this.toEmit = toEmit; }
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long i = 0L;
            while (running && i < toEmit) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(1L);
                }
                i++;
            }
            // 发送完成信号（若测试端有等待）
            @SuppressWarnings("unused")
            CountDownLatch latch = STATEFUL_PROCESSED_N_LATCH; // 仅为可见性，实际由算子完成
            // 进入空转等待，保持作业 RUNNING
            while (running) { Thread.sleep(50L); }
        }
        @Override
        public void cancel() { running = false; }
    }

    // --- 严格一致性探针 ---
    static volatile long TARGET_N = 0L;
    static volatile CountDownLatch STATEFUL_PROCESSED_N_LATCH = null;
    static volatile boolean PROBE_ONLY = false;
    static volatile CountDownLatch PROBE_LATCH = null;
    static final AtomicLong RESTORED_SNAPSHOT = new AtomicLong(-1L);

    /**
     * 有状态算子（带探测）：
     * - 常规模式：对固定 key 自增，处理到 TARGET_N 时倒计时；
     * - 探测模式：在收到首个元素时仅读取状态值并记录，不做更新。
     */
    public static class StatefulCounterWithProbe extends RichFlatMapFunction<Long, Long> {
        private transient ValueState<Long> sum;
        private transient boolean probed = false;
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> d = new ValueStateDescriptor<>("sum", Types.LONG);
            sum = getRuntimeContext().getState(d);
        }
        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            Long s = sum.value();
            if (s == null) s = 0L;
            // 探测模式：仅读取一次，不更新
            if (PROBE_ONLY && !probed) {
                RESTORED_SNAPSHOT.set(s);
                probed = true;
                CountDownLatch latch = PROBE_LATCH;
                if (latch != null) { latch.countDown(); }
                return;
            }
            // 常规累加
            s += 1L;
            sum.update(s);
            STATE_SUM_PROBE.set(s);
            CountDownLatch done = STATEFUL_PROCESSED_N_LATCH;
            if (done != null && s == TARGET_N) {
                done.countDown();
            }
        }
    }

    @Test
    void testPeriodicCheckpointCompletes() throws Exception {
        CHECKPOINT_COMPLETED.set(0);
        STATE_SUM_PROBE.set(0L);

        // 将 checkpoint 输出到临时目录，取消时保留；测试轮询该目录以确认生成了 chk-* 子目录
        Path ckBaseDir = Files.createTempDirectory("forl0-ck");
        String ckBaseUri = ckBaseDir.toUri().toString();

        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf.setString("state.checkpoints.dir", ckBaseUri);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(1);
        env.enableCheckpointing(200);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(50);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Long> stream = env
                .addSource(new InfiniteLongSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .name("src").uid("src");

        stream
            .keyBy(v -> 0)
            .flatMap(new StatefulCounter())
            .name("stateful").uid("stateful")
            .addSink(new DiscardingSink<>())
            .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client = env.executeAsync("periodic-checkpoint");
        final String jobIdStr = client.getJobID().toString();

        // 最多等待 25s，轮询 state.checkpoints.dir/<jobId>/ 下是否出现 chk-* 目录
        boolean found = false;
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        Path jobDir = Paths.get(new URI(ckBaseUri)).resolve(jobIdStr);
        while (System.nanoTime() < deadline) {
            if (Files.isDirectory(jobDir)) {
                try (Stream<Path> children = Files.list(jobDir)) {
                    found = children.anyMatch(p -> p.getFileName().toString().startsWith("chk-"));
                }
                if (found) break;
            }
            Thread.sleep(100);
        }

        try {
            client.cancel().get(10, TimeUnit.SECONDS);
        } catch (Throwable ignore) { }

        Assertions.assertTrue(found, "未检测到已完成的 checkpoint 目录");
    }

    @Test
    void testSavepointAndRestore() throws Exception {
        CHECKPOINT_COMPLETED.set(0);
        STATE_SUM_PROBE.set(0L);

        // 1) 启动作业并运行到一定进度
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(300);

        DataStream<Long> s1 = env1
                .addSource(new InfiniteLongSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .name("src").uid("src");

        s1.keyBy(v -> 0)
          .flatMap(new StatefulCounter())
          .name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>())
          .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("savepoint-first-run");

        // 等待状态累计到一定阈值
        long targetBefore = 1_000L;
        long deadline1 = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline1 && STATE_SUM_PROBE.get() < targetBefore) {
            Thread.sleep(20);
        }
        long savedAt = STATE_SUM_PROBE.get();
        Assertions.assertTrue(savedAt >= targetBefore, "作业应已累计到阈值");

        // 触发 savepoint 并取消作业
        Path spDir = Files.createTempDirectory("forl0-sp");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString())
                .get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) 基于 savepoint 恢复
        STATE_SUM_PROBE.set(0L); // 重置探针

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        // 直接使用配置键，避免依赖 SavepointConfigOptions 类
        conf2.setString("execution.savepoint.path", spPath);

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(300);

        DataStream<Long> s2 = env2
                .addSource(new InfiniteLongSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .name("src").uid("src");

        s2.keyBy(v -> 0)
          .flatMap(new StatefulCounter())
          .name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>())
          .name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("restore-from-savepoint");

        long targetAfter = savedAt + 500L;
        long deadline2 = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline2 && STATE_SUM_PROBE.get() < targetAfter) {
            Thread.sleep(20);
        }

        try {
            client2.cancel().get(10, TimeUnit.SECONDS);
        } catch (Throwable ignore) { }

        long after = STATE_SUM_PROBE.get();
        Assertions.assertTrue(after >= targetAfter, "恢复后状态应继续累加，期待 >= " + targetAfter + ", 实际=" + after);
    }

    @Test
    void testSavepointAndRestore_ExactState() throws Exception {
        // 精确一致性：先处理恰好 N 条，触发 savepoint；恢复后在探测元素到来前读取状态值，断言等于 N。
        final long N = 1_000L;
        TARGET_N = N;
        STATEFUL_PROCESSED_N_LATCH = new CountDownLatch(1);
        PROBE_ONLY = false;
        RESTORED_SNAPSHOT.set(-1L);

        // 1) 首次运行，发出恰好 N 条
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(200);

        DataStream<Long> s1 = env1
                .addSource(new ControlledSource(N)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s1.keyBy(v -> 0)
          .flatMap(new StatefulCounterWithProbe()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("sp-exact-1");

        // 等待状态处理到第 N 条
        boolean reached = STATEFUL_PROCESSED_N_LATCH.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(reached, "等待处理到第 N 条超时");

        // 触发 savepoint
        Path spDir = Files.createTempDirectory("forl0-sp-exact");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString()).get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) 恢复，使用“探测模式”：发 1 条探测元素，先读取状态值再进行任何更新
        PROBE_ONLY = true;
        PROBE_LATCH = new CountDownLatch(1);
        RESTORED_SNAPSHOT.set(-1L);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf2.setString("execution.savepoint.path", spPath);

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(200);

        DataStream<Long> s2 = env2
                .addSource(new ControlledSource(1)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s2.keyBy(v -> 0)
          .flatMap(new StatefulCounterWithProbe()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("sp-exact-2");

        boolean probed = PROBE_LATCH.await(20, TimeUnit.SECONDS);
        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}
        Assertions.assertTrue(probed, "恢复后探测超时");
        Assertions.assertEquals(N, RESTORED_SNAPSHOT.get(), "savepoint 恢复瞬间的状态值不一致");
    }

    @Test
    void testExternalizedCheckpoint_ExactState() throws Exception {
        // 精确一致性：先处理恰好 N 条并等待外部化 checkpoint 生成；恢复后探测状态值等于 N。
        final long N = 1_000L;
        TARGET_N = N;
        STATEFUL_PROCESSED_N_LATCH = new CountDownLatch(1);
        PROBE_ONLY = false;
        RESTORED_SNAPSHOT.set(-1L);

        // checkpoint 基目录（外部化保留）
        Path ckBaseDir = Files.createTempDirectory("forl0-ck-exact");
        String ckBaseUri = ckBaseDir.toUri().toString();

        // 1) 首次运行，发出恰好 N 条
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf1.setString("state.checkpoints.dir", ckBaseUri);
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(200);
        env1.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Long> s1 = env1
                .addSource(new ControlledSource(N)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s1.keyBy(v -> 0)
          .flatMap(new StatefulCounterWithProbe()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("ck-exact-1");
        final String jobIdStr = client1.getJobID().toString();

        // 等待状态处理到第 N 条
        boolean reached = STATEFUL_PROCESSED_N_LATCH.await(30, TimeUnit.SECONDS);
        Assertions.assertTrue(reached, "等待处理到第 N 条超时");

        // 轮询 job 目录下的 chk-*，拿到最新 checkpoint 路径
        Path jobDir = Paths.get(new URI(ckBaseUri)).resolve(jobIdStr);
        String ckRestorePath = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (System.nanoTime() < deadline && ckRestorePath == null) {
            if (Files.isDirectory(jobDir)) {
                try (Stream<Path> children = Files.list(jobDir)) {
                    Path latest = children
                            .filter(p -> p.getFileName().toString().startsWith("chk-"))
                            .max(Comparator.comparing(p -> p.getFileName().toString()))
                            .orElse(null);
                    if (latest != null) {
                        // 仅当存在 _metadata 文件时，认定 checkpoint 已完成且可恢复
                        Path meta = latest.resolve("_metadata");
                        if (Files.isRegularFile(meta)) {
                            ckRestorePath = latest.toUri().toString();
                        }
                    }
                }
            }
            if (ckRestorePath == null) { Thread.sleep(100); }
        }
        Assertions.assertNotNull(ckRestorePath, "未检测到已完成的 checkpoint 目录");

        // 取消作业，保留外部化 checkpoint
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) 恢复，使用“探测模式”：发 1 条探测元素，先读取状态值再进行任何更新
        PROBE_ONLY = true;
        PROBE_LATCH = new CountDownLatch(1);
        RESTORED_SNAPSHOT.set(-1L);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        // 使用 fromSavepoint 语义从外部化 checkpoint 恢复
        conf2.setString("execution.savepoint.path", ckRestorePath);

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(200);

        DataStream<Long> s2 = env2
                .addSource(new ControlledSource(1)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s2.keyBy(v -> 0)
          .flatMap(new StatefulCounterWithProbe()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("ck-exact-2");

        boolean probed = PROBE_LATCH.await(30, TimeUnit.SECONDS);
        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}
        Assertions.assertTrue(probed, "恢复后探测超时");
        Assertions.assertEquals(N, RESTORED_SNAPSHOT.get(), "checkpoint 恢复瞬间的状态值不一致");
    }

    // --- Savepoint compatibility probes for multi-state ---
    static volatile boolean COMPAT_PROBE_MODE = false;
    static volatile CountDownLatch COMPAT_PROBE_LATCH = null;
    static final AtomicLong COMPAT_VALUE_PROBE = new AtomicLong(-1);
    static final AtomicLong COMPAT_LIST_SIZE_PROBE = new AtomicLong(-1);
    static final AtomicLong COMPAT_MAP_TOTAL_PROBE = new AtomicLong(-1);

    /**
     * Stateful operator using Value + List + Map state for savepoint compatibility test.
     */
    public static class MultiTypeStatefulOp extends RichFlatMapFunction<Long, Long> {
        private transient ValueState<Long> counter;
        private transient ListState<Long> history;
        private transient MapState<Long, Long> indexed;
        private transient boolean probed = false;

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Types.LONG));
            history = getRuntimeContext().getListState(new ListStateDescriptor<>("hist", Types.LONG));
            indexed = getRuntimeContext().getMapState(new MapStateDescriptor<>("idx", Types.LONG, Types.LONG));
        }

        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            if (COMPAT_PROBE_MODE && !probed) {
                Long v = counter.value();
                COMPAT_VALUE_PROBE.set(v != null ? v : 0L);
                long listCount = 0;
                for (@SuppressWarnings("unused") Long x : history.get()) listCount++;
                COMPAT_LIST_SIZE_PROBE.set(listCount);
                long mapTotal = 0;
                for (java.util.Map.Entry<Long, Long> e : indexed.entries()) {
                    mapTotal += e.getValue();
                }
                COMPAT_MAP_TOTAL_PROBE.set(mapTotal);
                probed = true;
                CountDownLatch latch = COMPAT_PROBE_LATCH;
                if (latch != null) latch.countDown();
                return;
            }
            Long s = counter.value();
            if (s == null) s = 0L;
            s++;
            counter.update(s);
            history.add(s);
            indexed.put(s, s * 10);
            STATE_SUM_PROBE.set(s);
        }
    }

    /**
     * Savepoint compatibility: Value + List + MapState<Long,Long> checkpoint & restore.
     * Verifies that the optimized checkpoint format correctly preserves all state types.
     */
    @Test
    void testSavepointCompatibilityMultiStateTypes() throws Exception {
        final long N = 200L;
        STATE_SUM_PROBE.set(0);
        COMPAT_PROBE_MODE = false;

        // 1) First run
        Configuration conf1 = new Configuration();
        conf1.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.configure(conf1, Thread.currentThread().getContextClassLoader());
        env1.setParallelism(1);
        env1.enableCheckpointing(200);

        DataStream<Long> s1 = env1
                .addSource(new ControlledSource(N)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s1.keyBy(v -> 0)
          .flatMap(new MultiTypeStatefulOp()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client1 = env1.executeAsync("compat-1");

        // Wait for all records to be processed
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && STATE_SUM_PROBE.get() < N) {
            Thread.sleep(20);
        }
        Assertions.assertTrue(STATE_SUM_PROBE.get() >= N, "Should process all records");

        Path spDir = Files.createTempDirectory("forl0-sp-compat");
        String spPath = client1.triggerSavepoint(spDir.toUri().toString()).get(30, TimeUnit.SECONDS);
        client1.cancel().get(10, TimeUnit.SECONDS);

        // 2) Restore and probe
        COMPAT_PROBE_MODE = true;
        COMPAT_PROBE_LATCH = new CountDownLatch(1);
        COMPAT_VALUE_PROBE.set(-1);
        COMPAT_LIST_SIZE_PROBE.set(-1);
        COMPAT_MAP_TOTAL_PROBE.set(-1);

        Configuration conf2 = new Configuration();
        conf2.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf2.setString("execution.savepoint.path", spPath);
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.configure(conf2, Thread.currentThread().getContextClassLoader());
        env2.setParallelism(1);
        env2.enableCheckpointing(200);

        DataStream<Long> s2 = env2
                .addSource(new ControlledSource(1)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        s2.keyBy(v -> 0)
          .flatMap(new MultiTypeStatefulOp()).name("stateful").uid("stateful")
          .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        final org.apache.flink.core.execution.JobClient client2 = env2.executeAsync("compat-2");

        boolean probed = COMPAT_PROBE_LATCH.await(20, TimeUnit.SECONDS);
        try { client2.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}

        Assertions.assertTrue(probed, "Probe should complete");
        Assertions.assertEquals(N, COMPAT_VALUE_PROBE.get(),
                "ValueState should be restored to N");
        Assertions.assertEquals(N, COMPAT_LIST_SIZE_PROBE.get(),
                "ListState should have N entries");
        // Map entries: keys 1..N with values 10,20,...,N*10 → sum = 10 * N*(N+1)/2
        long expectedMapTotal = 10L * N * (N + 1) / 2;
        Assertions.assertEquals(expectedMapTotal, COMPAT_MAP_TOTAL_PROBE.get(),
                "MapState<Long,Long> total should match");
    }
}
