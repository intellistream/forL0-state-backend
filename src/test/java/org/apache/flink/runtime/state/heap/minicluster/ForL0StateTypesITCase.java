package org.apache.flink.runtime.state.heap.minicluster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * 覆盖五种 KeyedState（Value/List/Reducing/Aggregating/Map）的端到端正确性测试，
 * 通过 SPI 加载 ForL0StateBackend。
 * Note: Uses some deprecated Flink APIs for compatibility testing.
 */
@SuppressWarnings("deprecation")
public class ForL0StateTypesITCase {

    private StreamExecutionEnvironment setupEnv() {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.runtime.state.heap.ForL0StateBackendFactory");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(conf, Thread.currentThread().getContextClassLoader());
        env.setParallelism(2);
        return env;
    }

    @Test
    void testValueState() throws Exception {
        StreamExecutionEnvironment env = setupEnv();

        DataStream<String> words = env
                .fromElements("a", "b", "a", "c", "b", "a")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        SingleOutputStreamOperator<Tuple2<String, Integer>> out = words
                .keyBy((KeySelector<String, String>) v -> v)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private transient ValueState<Integer> count;
                    @Override
                    public void open(Configuration parameters) {
                        count = getRuntimeContext().getState(new ValueStateDescriptor<>("vs", Types.INT));
                    }
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer c = count.value();
                        if (c == null) c = 0;
                        c++;
                        count.update(c);
                        out.collect(Tuple2.of(value, c));
                    }
                });

        Map<String, Integer> finalCounts;
        try (CloseableIterator<Tuple2<String, Integer>> it = out.executeAndCollect()) {
            finalCounts = collectMaxPerKey(it);
        }
        Assertions.assertEquals(3, finalCounts.get("a"));
        Assertions.assertEquals(2, finalCounts.get("b"));
        Assertions.assertEquals(1, finalCounts.get("c"));
    }

    @Test
    void testListState() throws Exception {
        StreamExecutionEnvironment env = setupEnv();

        DataStream<String> words = env
                .fromElements("x", "y", "x", "z", "y", "x")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        SingleOutputStreamOperator<Tuple2<String, Integer>> out = words
                .keyBy((KeySelector<String, String>) v -> v)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private transient ListState<String> list;
                    @Override
                    public void open(Configuration parameters) {
                        list = getRuntimeContext().getListState(new ListStateDescriptor<>("ls", Types.STRING));
                    }
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        list.add(value);
                        int size = 0;
                        for (@SuppressWarnings("unused") String ignored : list.get()) size++;
                        out.collect(Tuple2.of(value, size));
                    }
                });

        Map<String, Integer> finalCounts;
        try (CloseableIterator<Tuple2<String, Integer>> it = out.executeAndCollect()) {
            finalCounts = collectMaxPerKey(it);
        }
        Assertions.assertEquals(3, finalCounts.get("x"));
        Assertions.assertEquals(2, finalCounts.get("y"));
        Assertions.assertEquals(1, finalCounts.get("z"));
    }

    @Test
    void testReducingState() throws Exception {
        StreamExecutionEnvironment env = setupEnv();

        DataStream<String> words = env
                .fromElements("a", "b", "a", "c", "b", "a")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        SingleOutputStreamOperator<Tuple2<String, Integer>> out = words
                .keyBy((KeySelector<String, String>) v -> v)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private transient ReducingState<Integer> sum;
                    @Override
                    public void open(Configuration parameters) {
                        sum = getRuntimeContext().getReducingState(
                                new ReducingStateDescriptor<>("rs", Integer::sum, Types.INT));
                    }
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        sum.add(1);
                        out.collect(Tuple2.of(value, sum.get()));
                    }
                });

        Map<String, Integer> finalCounts;
        try (CloseableIterator<Tuple2<String, Integer>> it = out.executeAndCollect()) {
            finalCounts = collectMaxPerKey(it);
        }
        Assertions.assertEquals(3, finalCounts.get("a"));
        Assertions.assertEquals(2, finalCounts.get("b"));
        Assertions.assertEquals(1, finalCounts.get("c"));
    }

    @Test
    void testAggregatingState() throws Exception {
        StreamExecutionEnvironment env = setupEnv();

        DataStream<String> words = env
                .fromElements("a", "b", "a", "c", "b", "a")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        SingleOutputStreamOperator<Tuple2<String, Integer>> out = words
                .keyBy((KeySelector<String, String>) v -> v)
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    private transient AggregatingState<Integer, Integer> aggr;
                    @Override
                    public void open(Configuration parameters) {
                        aggr = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<>(
                                        "as",
                                        new AggregateFunctionImpl(),
                                        Types.INT));
                    }
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        aggr.add(1);
                        out.collect(Tuple2.of(value, aggr.get()));
                    }
                });

        Map<String, Integer> finalCounts;
        try (CloseableIterator<Tuple2<String, Integer>> it = out.executeAndCollect()) {
            finalCounts = collectMaxPerKey(it);
        }
        Assertions.assertEquals(3, finalCounts.get("a"));
        Assertions.assertEquals(2, finalCounts.get("b"));
        Assertions.assertEquals(1, finalCounts.get("c"));
    }

    @Test
    void testMapState() throws Exception {
        StreamExecutionEnvironment env = setupEnv();

        List<Tuple3<String, String, Integer>> inputs = Arrays.asList(
                Tuple3.of("k1", "x", 1),
                Tuple3.of("k1", "y", 1),
                Tuple3.of("k1", "x", 1),
                Tuple3.of("k2", "x", 1),
                Tuple3.of("k2", "z", 1)
        );

        DataStream<Tuple3<String, String, Integer>> ds = env
                .fromCollection(inputs)
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> out = ds
                .keyBy((KeySelector<Tuple3<String, String, Integer>, String>) v -> v.f0)
                .flatMap(new RichFlatMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    private transient MapState<String, Integer> map;
                    @Override
                    public void open(Configuration parameters) {
                        map = getRuntimeContext().getMapState(new MapStateDescriptor<>("ms", Types.STRING, Types.INT));
                    }
                    @Override
                    public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Integer cur = map.get(value.f1);
                        if (cur == null) cur = 0;
                        cur += value.f2;
                        map.put(value.f1, cur);
                        out.collect(Tuple3.of(value.f0, value.f1, cur));
                    }
                });

        // 收集最终 (key, field) 的最大计数
        Map<String, Map<String, Integer>> finalCounts = new HashMap<>();
        try (CloseableIterator<Tuple3<String, String, Integer>> it = out.executeAndCollect()) {
            while (it.hasNext()) {
                Tuple3<String, String, Integer> r = it.next();
                finalCounts.computeIfAbsent(r.f0, k -> new HashMap<>()).merge(r.f1, r.f2, Math::max);
            }
        }
        Assertions.assertEquals(2, finalCounts.get("k1").get("x").intValue());
        Assertions.assertEquals(1, finalCounts.get("k1").get("y").intValue());
        Assertions.assertEquals(1, finalCounts.get("k2").get("x").intValue());
        Assertions.assertEquals(1, finalCounts.get("k2").get("z").intValue());
    }

    private static Map<String, Integer> collectMaxPerKey(CloseableIterator<Tuple2<String, Integer>> it) {
        Map<String, Integer> max = new HashMap<>();
        while (it.hasNext()) {
            Tuple2<String, Integer> t = it.next();
            max.merge(t.f0, t.f1, Math::max);
        }
        return max;
    }

    // 简单整型累加器
    private static class AggregateFunctionImpl implements org.apache.flink.api.common.functions.AggregateFunction<Integer, Integer, Integer> {
        @Override public Integer createAccumulator() { return 0; }
        @Override public Integer add(Integer value, Integer acc) { return acc + value; }
        @Override public Integer getResult(Integer acc) { return acc; }
        @Override public Integer merge(Integer a, Integer b) { return a + b; }
    }
}

