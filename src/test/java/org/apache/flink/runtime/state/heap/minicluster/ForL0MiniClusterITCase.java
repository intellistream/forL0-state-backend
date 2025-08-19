package org.apache.flink.runtime.state.heap.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.util.concurrent.atomic.AtomicLong;
// 新增导入
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;

/**
 * MiniCluster 集成测试：使用 SPI 加载 ForL0StateBackend，验证 ValueState 读写。
 */
public class ForL0MiniClusterITCase {

    // 压测参数（直接改这里即可）
    private static final int PARALLELISM = 4;
    private static final long NUM_RECORDS = 20_000_000L;
    private static final int KEY_SPACE = 1_000_000;
    private static final double HOT_TRAFFIC_RATIO = 0.8;
    private static final double HOT_KEY_FRACTION = 0.01;
    private static final int EMIT_EVERY = 1000;

    // MiniCluster 配置：提升托管内存，避免 MemoryManager 分配失败
    private static final Configuration CLUSTER_CONF = new Configuration();
    static {
        // 提高托管内存与网络内存，避免 MemoryManager 分配失败
        CLUSTER_CONF.setString("taskmanager.memory.managed.size", "12gb");
        CLUSTER_CONF.setString("taskmanager.memory.network.min", "128mb");
        CLUSTER_CONF.setString("taskmanager.memory.network.max", "128mb");
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(PARALLELISM + 2)
                    .setConfiguration(CLUSTER_CONF)
                    .build());

    @Test
    void testValueStateWordCountWithSPI() throws Exception {
        // 配置通过 SPI 使用自定义 StateBackend
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.runtime.state.heap.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(2);

        // 构造一个有界输入
        DataStream<String> lines = env
                .fromElements("a a b", "b c", "a c c")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        // 拆词 -> keyBy -> 使用 ValueState 计数
        DataStream<String> words = lines.flatMap((String s, Collector<String> out) -> {
            for (String w : s.split(" ")) {
                if (!w.isEmpty()) {
                    out.collect(w);
                }
            }
        }).returns(Types.STRING);

        SingleOutputStreamOperator<Tuple2<String, Integer>> counted = words
                .keyBy(v -> v)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> count;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>(
                                "cnt", Types.INT);
                        count = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer c = count.value();
                        if (c == null) { c = 0; }
                        c += 1;
                        count.update(c);
                        out.collect(Tuple2.of(value, c));
                    }
                });

        // 执行并收集全部结果
        List<Tuple2<String, Integer>> results = new ArrayList<>();
        try (CloseableIterator<Tuple2<String, Integer>> it = counted.executeAndCollect()) {
            while (it.hasNext()) {
                results.add(it.next());
            }
        }

        // 计算最终每个 key 的最大计数（即总次数）
        Map<String, Integer> finalCounts = results.stream()
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1, Math::max));

        Assertions.assertEquals(3, finalCounts.get("a"));
        Assertions.assertEquals(2, finalCounts.get("b"));
        Assertions.assertEquals(3, finalCounts.get("c"));
    }

    @Test
    void testSkewedHighLoadValueState() throws Exception {
        // 使用类级常量
        final long numRecords = NUM_RECORDS;
        final int keySpace = KEY_SPACE;
        final double hotTrafficRatio = HOT_TRAFFIC_RATIO;
        final double hotKeyFraction = HOT_KEY_FRACTION;
        final int emitEvery = EMIT_EVERY;
        final int parallelism = PARALLELISM;

        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.runtime.state.heap.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(parallelism);

        // 基础有界序列，最大速率推动
        DataStream<Long> base = env.fromSequence(0, numRecords - 1).slotSharingGroup("default");

        // 统计总处理条数（不影响状态逻辑），避免把全部结果拉回测试进程
        base
            .map(new RichMapFunction<Long, Long>() {
                @Override
                public Long map(Long value) { return 1L; }
            }).slotSharingGroup("default")
            .keyBy(v -> 0)
            .reduce(Long::sum).slotSharingGroup("default")
            .addSink(new AccumulatorSink()).slotSharingGroup("default");

        // 将序列映射为倾斜 key
        long hotKeys = Math.max(1, Math.round(keySpace * hotKeyFraction));
        long coldKeys = Math.max(1, keySpace - hotKeys);
        final long skewDen = 1_000_000L;
        final long skewNum = Math.min(skewDen - 1, Math.max(0L, Math.round(skewDen * hotTrafficRatio)));

        DataStream<String> words = base.map(i -> {
            // 确定性倾斜：按比例把记录映射到热区/冷区
            boolean toHot = (i % skewDen) < skewNum;
            long keyIdx;
            if (toHot) {
                keyIdx = i % hotKeys; // 热区均匀分配
            } else {
                keyIdx = hotKeys + (i % coldKeys); // 冷区均匀分配
            }
            return "k" + keyIdx;
        }).slotSharingGroup("default");

        // 使用 ValueState 自增计数，采样输出，减轻收集压力（高负载分支保留，但丢弃输出）
        SingleOutputStreamOperator<Tuple2<String, Integer>> countedSampled = words
            .keyBy(v -> v)
            .flatMap(new org.apache.flink.api.common.functions.RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                private transient ValueState<Integer> count;

                @Override
                public void open(Configuration parameters) {
                    ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("cnt", Types.INT);
                    count = getRuntimeContext().getState(desc);
                }

                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    Integer c = count.value();
                    if (c == null) { c = 0; }
                    c += 1;
                    count.update(c);
                    if (c % emitEvery == 0) {
                        out.collect(Tuple2.of(value, c));
                    }
                }
            }).slotSharingGroup("default");
        // 不回传到测试端，避免海量数据
        countedSampled.addSink(new DiscardingSink<>()).slotSharingGroup("default");

        // ---------------- 小样本正确性校验分支 ----------------
        // 选择少量确定性 key：前 N 个热 key 与前 N 个冷 key
        int sampleNHot = (int) Math.min(5, hotKeys);
        int sampleNCold = (int) Math.min(5, coldKeys);
        Set<String> sampleKeys = new HashSet<>();
        for (int i = 0; i < sampleNHot; i++) {
            sampleKeys.add("k" + i);
        }
        for (int i = 0; i < sampleNCold; i++) {
            sampleKeys.add("k" + (hotKeys + i));
        }

        DataStream<String> wordsSample = words
            .filter(sampleKeys::contains)
            .slotSharingGroup("default");

        // a) ValueState 分支：对样本 key 每条都输出，得到最终计数
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> vsCounts = wordsSample
            .keyBy(v -> v)
            .flatMap(new org.apache.flink.api.common.functions.RichFlatMapFunction<String, Tuple3<String, Integer, String>>() {
                private transient ValueState<Integer> count;
                @Override
                public void open(Configuration parameters) {
                    ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("cnt_verify", Types.INT);
                    count = getRuntimeContext().getState(desc);
                }
                @Override
                public void flatMap(String value, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                    Integer c = count.value();
                    if (c == null) { c = 0; }
                    c += 1;
                    count.update(c);
                    out.collect(Tuple3.of(value, c, "vs"));
                }
            }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
            .slotSharingGroup("default");

        // b) 基线分支：sum(1) 得到真实最终计数（流端输出递增值）
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> aggCounts = wordsSample
            .map(k -> Tuple2.of(k, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> Tuple3.of(t.f0, t.f1, "agg"))
            .returns(Types.TUPLE(Types.STRING, Types.INT, Types.STRING))
            .slotSharingGroup("default");

        // 合并两路样本流，集中收集
        DataStream<Tuple3<String, Integer, String>> unioned = vsCounts.union(aggCounts);

        // 触发执行并消费样本校验流；同时高负载分支仍在执行但输出被丢弃
        Map<String, Integer> vsMax = new HashMap<>();
        Map<String, Integer> aggMax = new HashMap<>();
        try (CloseableIterator<Tuple3<String, Integer, String>> it = unioned.executeAndCollect()) {
            while (it.hasNext()) {
                Tuple3<String, Integer, String> e = it.next();
                if ("vs".equals(e.f2)) {
                    vsMax.merge(e.f0, e.f1, Math::max);
                } else if ("agg".equals(e.f2)) {
                    aggMax.merge(e.f0, e.f1, Math::max);
                }
            }
        }

        // 对于样本 key，ValueState 最终计数应与 sum(1) 的最终计数一致
        for (String k : sampleKeys) {
            Integer v1 = vsMax.get(k);
            Integer v2 = aggMax.get(k);
            Assertions.assertNotNull(v1, "VS missing key: " + k);
            Assertions.assertNotNull(v2, "AGG missing key: " + k);
            Assertions.assertEquals(v2.intValue(), v1.intValue(), "count mismatch for key " + k);
        }

        // 验证总处理条数（由静态 Sink 汇总）
        Assertions.assertEquals(numRecords, AccumulatorSink.TOTAL_PROCESSED.get());
    }

    @Test
    void testSlidingEventTimeWindowWordCount() throws Exception {
        // 使用自定义 StateBackend（SPI）
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.runtime.state.heap.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(2);

        // 准备小样本事件（word, eventTimeMillis）
        List<Tuple2<String, Long>> events = new ArrayList<>();
        // 窗口长度10s，滑动5s；事件时间单位毫秒
        events.add(Tuple2.of("a", 1_000L));   // 1s
        events.add(Tuple2.of("b", 2_000L));   // 2s
        events.add(Tuple2.of("a", 4_000L));   // 4s
        events.add(Tuple2.of("a", 6_000L));   // 6s
        events.add(Tuple2.of("b", 11_000L));  // 11s -> 触发 [0,10) 窗口出结果
        events.add(Tuple2.of("a", 16_000L));  // 16s

        DataStream<Tuple2<String, Long>> input = env
                .fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f1)
                );

        // 窗口统计：输出 (word, windowEnd, count)
        SingleOutputStreamOperator<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> windowed = input
                .keyBy(e -> e.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Long>, org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<Tuple2<String, Long>> input,
                                      Collector<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> out) {
                        int cnt = 0;
                        for (Tuple2<String, Long> ignored : input) { cnt++; }
                        out.collect(org.apache.flink.api.java.tuple.Tuple3.of(key, window.getEnd(), cnt));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT));

        // 收集全部窗口结果
        List<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> results = new ArrayList<>();
        try (CloseableIterator<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> it = windowed.executeAndCollect()) {
            while (it.hasNext()) {
                results.add(it.next());
            }
        }

        // 计算每个 (word, windowEnd) 的最大计数（即该窗口最终输出）
        Map<String, Map<Long, Integer>> actual = new HashMap<>();
        for (org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer> r : results) {
            actual.computeIfAbsent(r.f0, k -> new HashMap<>()).merge(r.f1, r.f2, Math::max);
        }

        // 期望：窗口长度10s，滑动5s，窗口右边界分别为 10s, 15s, 20s, 25s（单位毫秒）
        long w10 = 10_000L, w15 = 15_000L, w20 = 20_000L, w25 = 25_000L;
        // [0,10): a=3 (1s,4s,6s), b=1 (2s)
        Assertions.assertEquals(3, actual.getOrDefault("a", Collections.emptyMap()).get(w10));
        Assertions.assertEquals(1, actual.getOrDefault("b", Collections.emptyMap()).get(w10));
        // [5,15): a=1 (6s), b=1 (11s)
        Assertions.assertEquals(1, actual.getOrDefault("a", Collections.emptyMap()).get(w15));
        Assertions.assertEquals(1, actual.getOrDefault("b", Collections.emptyMap()).get(w15));
        // [10,20): a=1 (16s), b=1 (11s)
        Assertions.assertEquals(1, actual.getOrDefault("a", Collections.emptyMap()).get(w20));
        Assertions.assertEquals(1, actual.getOrDefault("b", Collections.emptyMap()).get(w20));
        // [15,25): a=1 (16s)
        Assertions.assertEquals(1, actual.getOrDefault("a", Collections.emptyMap()).get(w25));
        Assertions.assertNull(actual.getOrDefault("b", Collections.emptyMap()).get(w25));
    }

    @Test
    void testSlidingEventTimeWindowWordCount_Stress() throws Exception {
        // 使用自定义 StateBackend（SPI）
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.runtime.state.heap.ForL0StateBackendFactory");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(2);

        // 生成较大样本：持续 300 秒，a 每 10ms 1 条，b 每 20ms 1 条（单调事件时间，保证单调水位线）
        final long durationMs = 300_000L; // 300s
        final long stepA = 10L;  // 10ms 一条
        final long stepB = 20L;  // 20ms 一条
        List<Tuple2<String, Long>> events = new ArrayList<>();
        for (long t = stepA; t <= durationMs; t += stepA) {
            events.add(Tuple2.of("a", t));
            if (t % stepB == 0) {
                events.add(Tuple2.of("b", t));
            }
        }

        DataStream<Tuple2<String, Long>> input = env
                .fromCollection(events)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((e, ts) -> e.f1)
                );

        // 滑动窗口 10s，每 5s 触发；输出 (word, windowEnd, count)
        SingleOutputStreamOperator<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> windowed = input
                .keyBy(e -> e.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Long>, org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<Tuple2<String, Long>> input,
                                      Collector<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> out) {
                        int cnt = 0;
                        for (Tuple2<String, Long> ignored : input) { cnt++; }
                        out.collect(org.apache.flink.api.java.tuple.Tuple3.of(key, window.getEnd(), cnt));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT));

        // 收集窗口结果 -> 实际值
        List<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> results = new ArrayList<>();
        try (CloseableIterator<org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer>> it = windowed.executeAndCollect()) {
            while (it.hasNext()) {
                results.add(it.next());
            }
        }
        Map<String, Map<Long, Integer>> actual = new HashMap<>();
        for (org.apache.flink.api.java.tuple.Tuple3<String, Long, Integer> r : results) {
            actual.computeIfAbsent(r.f0, k -> new HashMap<>()).merge(r.f1, r.f2, Math::max);
        }

        // 基于输入事件离线计算期望值（严格按照 [start, end) 规则）
        Map<String, List<Long>> tsByKey = new HashMap<>();
        for (Tuple2<String, Long> e : events) {
            tsByKey.computeIfAbsent(e.f0, k -> new ArrayList<>()).add(e.f1);
        }
        long slide = 5_000L, size = 10_000L;
        long endMax = (durationMs / slide) * slide;
        Map<String, Map<Long, Integer>> expected = new HashMap<>();
        for (String key : tsByKey.keySet()) {
            List<Long> ts = tsByKey.get(key);
            int left = 0, right = 0;
            Map<Long, Integer> m = new HashMap<>();
            for (long end = size; end <= endMax; end += slide) {
                long start = end - size;
                while (left < ts.size() && ts.get(left) < start) left++;
                while (right < ts.size() && ts.get(right) < end) right++;
                m.put(end, right - left);
            }
            expected.put(key, m);
        }

        // 断言：每个 (key, windowEnd) 的计数与期望一致
        for (Map.Entry<String, Map<Long, Integer>> ke : expected.entrySet()) {
            String key = ke.getKey();
            Map<Long, Integer> expWin = ke.getValue();
            Map<Long, Integer> actWin = actual.getOrDefault(key, Collections.emptyMap());
            for (Map.Entry<Long, Integer> we : expWin.entrySet()) {
                Long end = we.getKey();
                Integer exp = we.getValue();
                Integer act = actWin.get(end);
                Assertions.assertNotNull(act, "missing window result for key=" + key + ", end=" + end);
                Assertions.assertEquals(exp.intValue(), act.intValue(), "mismatch for key=" + key + ", end=" + end);
            }
        }
    }

    // 累加器 Sink：保存最后的聚合计数到静态变量，供断言使用
    public static class AccumulatorSink extends RichSinkFunction<Long> {
        static final AtomicLong TOTAL_PROCESSED = new AtomicLong();
        @Override
        public void invoke(Long value) {
            TOTAL_PROCESSED.set(value);
        }
    }
}
