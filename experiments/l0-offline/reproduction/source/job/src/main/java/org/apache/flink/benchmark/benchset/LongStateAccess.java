package org.apache.flink.benchmark.benchset;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;

/** Identical standard Flink state calls for every configured backend. */
public final class LongStateAccess {
    public static final class Vector {
        private final MapState<Long, Long> state;
        public Vector(MapState<Long, Long> state) { this.state = state; }
        public long increment(int start, int touches, int width) throws Exception {
            long sum = 0;
            for (int i = 0; i < touches; i++) {
                long slot = (start + (long) i) % width;
                Long old = state.get(slot);
                long next = (old == null ? 0 : old) + 1_000_000_007L;
                state.put(slot, next);
                sum += next;
            }
            return sum;
        }
    }
    public static final class Counter {
        private final ValueState<Long> state;
        public Counter(ValueState<Long> state) { this.state = state; }
        public long add(long delta) throws Exception {
            Long old = state.value();
            long next = (old == null ? 0 : old) + delta;
            state.update(next);
            return next;
        }
        public long get() throws Exception { Long value = state.value(); return value == null ? 0 : value; }
        public void set(long value) throws Exception { state.update(value); }
    }
    private LongStateAccess() { }
}
