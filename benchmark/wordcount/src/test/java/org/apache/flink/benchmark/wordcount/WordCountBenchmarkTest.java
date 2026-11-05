package org.apache.flink.benchmark.wordcount;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class WordCountBenchmarkTest {
    private RuntimeContext context(ValueState<Long> state) {
        return (RuntimeContext) Proxy.newProxyInstance(RuntimeContext.class.getClassLoader(),
                new Class<?>[]{RuntimeContext.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getState")) return state;
                    throw new UnsupportedOperationException(method.getName());
                });
    }
    private <K> Collector<Tuple2<K, Long>> collector(List<Long> values) {
        return new Collector<Tuple2<K, Long>>() {
            @Override public void collect(Tuple2<K, Long> value) { values.add(value.f1); }
            @Override public void close() { }
        };
    }
    public static class BasicHeapState implements ValueState<Long> {
        Long value;
        int reads;
        @Override public Long value() { reads++; return value; }
        @Override public void update(Long next) { value = next; }
        @Override public void clear() { value = null; }
    }
    @Test void longHeapFallbackPerformsTheSameNumberOfUpdates() throws Exception {
        BasicHeapState state = new BasicHeapState();
        WordCountBenchmark.LongKeyStatefulCounter function = new WordCountBenchmark.LongKeyStatefulCounter(8);
        function.setRuntimeContext(context(state));
        function.open(new Configuration());
        List<Long> values = new ArrayList<>();
        function.processElement(Tuple2.of(1L, 1L), null, collector(values));
        function.processElement(Tuple2.of(1L, 1L), null, collector(values));
        assertEquals(java.util.Arrays.asList(8L, 16L), values);
        assertEquals(16, state.reads);
        assertEquals(Long.valueOf(16), state.value);
    }

    @Test void independentCountOracleRejectsMissingUpdates() throws Exception {
        org.apache.flink.benchmark.wordcount.sink.MetricsSink<Long> sink =
                new org.apache.flink.benchmark.wordcount.sink.MetricsSink<>(null, 8, "test", 3, 8);
        sink.open(new Configuration());
        sink.invoke(Tuple2.of(1L, 8L), null);
        sink.invoke(Tuple2.of(2L, 8L), null);
        sink.invoke(Tuple2.of(1L, 16L), null);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> sink.invoke(Tuple2.of(1L, 32L), null));
    }

}
