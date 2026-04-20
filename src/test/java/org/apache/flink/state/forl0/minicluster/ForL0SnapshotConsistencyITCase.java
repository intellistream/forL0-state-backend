package org.apache.flink.state.forl0.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 验证启用 L0 HotCache 时 savepoint 的一致性：
 * 快照必须始终读取 SwissTable（权威存储），HotCache 仅作为读缓存。
 *
 * <p>HotCache 的设计约束：write-through + non-authoritative。
 *   任何只写入 cache 但未下沉到 SwissTable 的情况都会在快照/恢复时暴露。
 *   本测试显式开启 {@code state.backend.forl0.l0-cache.enabled=true}，
 *   在 macOS 上硬件网关关闭、cache 被强制禁用，行为应与 cache-off 一致；
 *   在鲲鹏上 cache 实际生效，用于检测回归。</p>
 */
@SuppressWarnings("deprecation")
public class ForL0SnapshotConsistencyITCase {

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
    static volatile long UPDATES_PER_KEY = 0L;
    static volatile boolean PROBE_ONLY = false;
    static volatile boolean CLEAR_AFTER_WRITE = false;
    static volatile CountDownLatch WRITE_DONE_LATCH = null;
    static volatile CountDownLatch PROBE_DONE_LATCH = null;
    static final AtomicLong PROCESSED = new AtomicLong(0L);
    static final AtomicInteger PROBE_COUNT = new AtomicInteger(0);
    static final Map<Long, Long> PROBED_VALUE = new ConcurrentHashMap<>();
    static final Map<Long, List<Long>> PROBED_LIST = new ConcurrentHashMap<>();

    public static class MultiKeySource extends RichParallelSourceFunction<Tuple2<Long, Integer>> {
        private volatile boolean running = true;
        private final int numKeys;
        private final long updatesPerKey;

        public MultiKeySource(int numKeys, long updatesPerKey) {
            this.numKeys = numKeys;
            this.updatesPerKey = updatesPerKey;
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Integer>> ctx) throws Exception {
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            long r = 0;
            while (running && r < updatesPerKey) {
                for (long key = 0; key < numKeys && running; key++) {
                    if ((int) (key % parallelism) != taskIdx) continue;
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(Tuple2.of(key, 1));
                    }
                }
                r++;
            }
            while (running) { Thread.sleep(20L); }
        }

        @Override
        public void cancel() { running = false; }
    }

    /** 同时维护 ValueState 与 ListState — 双状态快照一致性验证. */
    public static class ValueAndListOp
            extends RichFlatMapFunction<Tuple2<Long, Integer>, Long> {
        private transient ValueState<Long> counter;
        private transient ListState<Long> history;
        private transient boolean[] probedSeen;

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("snap_counter", Types.LONG));
            history = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("snap_history", Types.LONG));
            probedSeen = new boolean[Math.max(NUM_KEYS, 1)];
        }

        @Override
        public void flatMap(Tuple2<Long, Integer> in, Collector<Long> out) throws Exception {
            Long key = in.f0;
            if (PROBE_ONLY) {
                int idx = key.intValue();
                if (idx < 0 || idx >= probedSeen.length || probedSeen[idx]) return;
                Long v = counter.value();
                PROBED_VALUE.put(key, v == null ? -1L : v);
                List<Long> snapshot = new ArrayList<>();
                for (Long e : history.get()) snapshot.add(e);
                PROBED_LIST.put(key, snapshot);
                probedSeen[idx] = true;
                int done = PROBE_COUNT.incrementAndGet();
                CountDownLatch latch = PROBE_DONE_LATCH;
                if (latch != null && done >= NUM_KEYS) latch.countDown();
                return;
            }

            Long v = counter.value();
            if (v == null) v = 0L;
            v += 1L;
            counter.update(v);
            history.add(v);
            long n = PROCESSED.incrementAndGet();
            if (CLEAR_AFTER_WRITE && v == UPDATES_PER_KEY) {
                // 对达到预定次数的 key，做一次 clear — 快照必须反映“已清空”状态
                counter.clear();
                history.clear();
            }
            CountDownLatch latch = WRITE_DONE_LATCH;
            if (latch != null && n >= (long) NUM_KEYS * UPDATES_PER_KEY) {
                latch.countDown();
            }
        }
    }

    private static Configuration cacheEnabledConf() {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forl0.ForL0StateBackendFactory");
        conf.setString("state.backend.forl0.l0-cache.enabled", "true");
        conf.setString("state.backend.forl0.l0-cache.size", "32mb");
        return conf;
    }

    private static void resetProbes(int numKeys, long updates, boolean clearAfter) {
        NUM_KEYS = numKeys;
        UPDATES_PER_KEY = updates;
        PROBE_ONLY = false;
        CLEAR_AFTER_WRITE = clearAfter;
        WRITE_DONE_LATCH = new CountDownLatch(1);
        PROBE_DONE_LATCH = null;
        PROCESSED.set(0L);
        PROBE_COUNT.set(0);
        PROBED_VALUE.clear();
        PROBED_LIST.clear();
    }

    private static String writeAndSavepoint(int parallelism, String spDirUri) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(cacheEnabledConf(), Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new MultiKeySource(NUM_KEYS, UPDATES_PER_KEY)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        src.keyBy(t -> t.f0)
           .flatMap(new ValueAndListOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("snap-write");
        boolean ok = WRITE_DONE_LATCH.await(40, TimeUnit.SECONDS);
        Assertions.assertTrue(ok, "写阶段超时");

        String spPath = client.triggerSavepoint(spDirUri).get(40, TimeUnit.SECONDS);
        client.cancel().get(10, TimeUnit.SECONDS);
        return spPath;
    }

    private static void restoreAndProbe(int parallelism, String savepointPath) throws Exception {
        PROBE_ONLY = true;
        PROBE_DONE_LATCH = new CountDownLatch(1);
        PROBE_COUNT.set(0);
        PROBED_VALUE.clear();
        PROBED_LIST.clear();

        Configuration conf = cacheEnabledConf();
        conf.setString("execution.savepoint.path", savepointPath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Long, Integer>> src = env
                .addSource(new MultiKeySource(NUM_KEYS, 1L)).name("src").uid("src")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        src.keyBy(t -> t.f0)
           .flatMap(new ValueAndListOp()).name("stateful").uid("stateful")
           .addSink(new DiscardingSink<>()).name("sink").uid("sink");

        org.apache.flink.core.execution.JobClient client = env.executeAsync("snap-probe");
        boolean ok = PROBE_DONE_LATCH.await(40, TimeUnit.SECONDS);
        try { client.cancel().get(10, TimeUnit.SECONDS); } catch (Throwable ignore) {}
        Assertions.assertTrue(ok,
                "探测阶段未覆盖所有 key: count=" + PROBE_COUNT.get() + ", expected=" + NUM_KEYS);
    }

    // -----------------------------------------------------------------------

    @Test
    void snapshotRestore_CacheEnabled_PreservesValueAndList() throws Exception {
        final int numKeys = 16;
        final long updates = 100L;
        resetProbes(numKeys, updates, false);
        Path spDir = Files.createTempDirectory("forl0-snap-cons-vl");

        String spPath = writeAndSavepoint(2, spDir.toUri().toString());
        restoreAndProbe(2, spPath);

        List<Long> expectedList = new ArrayList<>();
        for (long i = 1; i <= updates; i++) expectedList.add(i);

        for (long k = 0; k < numKeys; k++) {
            Long v = PROBED_VALUE.get(k);
            Assertions.assertNotNull(v, "key=" + k + " 未被探测");
            Assertions.assertEquals(updates, v.longValue(),
                    "ValueState 值不符合快照：key=" + k);
            List<Long> list = PROBED_LIST.get(k);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(expectedList, list,
                    "ListState 内容不符合快照：key=" + k);
        }
    }

    @Test
    void snapshotAfterClear_PreservesEmpty() throws Exception {
        // 关键场景：key 写入后被 clear()，cache 中 *可能* 尚未被驱逐但 SwissTable 已删除。
        // 快照必须反映 SwissTable 的真实状态（key 不存在），而非 cache 的旧值。
        final int numKeys = 8;
        final long updates = 50L;
        resetProbes(numKeys, updates, /* clearAfter */ true);
        Path spDir = Files.createTempDirectory("forl0-snap-cons-clear");

        String spPath = writeAndSavepoint(1, spDir.toUri().toString());
        restoreAndProbe(1, spPath);

        for (long k = 0; k < numKeys; k++) {
            Long v = PROBED_VALUE.get(k);
            Assertions.assertNotNull(v, "key=" + k + " 未被探测");
            // -1L 是探测返回的 null 哨兵
            Assertions.assertEquals(-1L, v.longValue(),
                    "clear() 后的 key 恢复时必须看到空状态：key=" + k + ", value=" + v);
            List<Long> list = PROBED_LIST.get(k);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(0, list.size(),
                    "clear() 后的 ListState 恢复时必须为空：key=" + k);
        }
    }

    @Test
    void snapshotRestore_CacheEnabled_SingleHotKey() throws Exception {
        // 极高的单 key 更新频率，确保 cache 命中率最大化；快照仍须得到权威值。
        final int numKeys = 1;
        final long updates = 5_000L;
        resetProbes(numKeys, updates, false);
        Path spDir = Files.createTempDirectory("forl0-snap-cons-hot");

        String spPath = writeAndSavepoint(1, spDir.toUri().toString());
        restoreAndProbe(1, spPath);

        Assertions.assertEquals(updates, PROBED_VALUE.get(0L).longValue(),
                "hot key 快照值错误");
        // ListState 应包含 1..updates，验证快照未受 cache 影响
        List<Long> list = PROBED_LIST.get(0L);
        Assertions.assertNotNull(list);
        Assertions.assertEquals((int) updates, list.size());
        for (int i = 0; i < list.size(); i++) {
            Assertions.assertEquals(i + 1L, list.get(i).longValue(),
                    "hot key ListState 顺序错误 at i=" + i);
        }
        // 静默引用避免被 IDE 去除
        Arrays.asList("ok").size();
    }
}
