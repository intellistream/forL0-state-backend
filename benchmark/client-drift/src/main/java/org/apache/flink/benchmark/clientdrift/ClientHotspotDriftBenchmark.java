package org.apache.flink.benchmark.clientdrift;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.example.HuaweiMT6000c;
import org.example.HuaweiTestFunction;
import org.example.PVMVLogType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Non-contract client-usecase variant that keeps the customer's join/state logic but
 * replaces fixed CSV replay with a drifting-hot-key source.
 */
public final class ClientHotspotDriftBenchmark {

    private ClientHotspotDriftBenchmark() {
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    private static int positiveMod(long value, int modulo) {
        int result = (int) (value % modulo);
        return result < 0 ? result + modulo : result;
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String mode = params.get("mode", "customer");
        int parallelism = params.getInt("parallelism", env.getParallelism());
        long checkpointInterval = params.getLong("checkpointInterval", 0L);
        long leftNumRecords = params.getLong("leftNumRecords", 0L);
        long rightNumRecords = params.getLong("rightNumRecords", 0L);
        int hotKeyCount = params.getInt("hotKeyCount", 2048);
        int coldKeyCount = params.getInt("coldKeyCount", 200000);
        int hotTrafficPercent = params.getInt("hotTrafficPercent", 80);
        long driftIntervalRecords = params.getLong("driftIntervalRecords", 20000L);
        int driftStep = params.getInt("driftStep", Math.max(1, hotKeyCount / 2));
        int rightDelayMs = params.getInt("rightDelayMs", 2);
        String payloadMode = params.get("payloadMode", "realistic");
        String timestampMode = params.get("timestampMode", "monotonic");
        int timestampHotBuckets = params.getInt("timestampHotBuckets", 0);
        int timestampKeySpace = params.getInt("timestampKeySpace", Math.max(1, coldKeyCount));
        long timestampDriftIntervalRecords = params.getLong("timestampDriftIntervalRecords", driftIntervalRecords);
        int timestampDriftStep = params.getInt("timestampDriftStep", Math.max(1, Math.max(1, timestampHotBuckets) / 2));

        env.setParallelism(parallelism);
        if (checkpointInterval > 0L) {
            env.enableCheckpointing(checkpointInterval);
        }

        if ("scalar_state".equalsIgnoreCase(mode)) {
            int scalarOpsPerRecord = params.getInt("scalarOpsPerRecord", 8);
            int mapKeyModulo = params.getInt("mapKeyModulo", 65536);
            String scalarShape = params.get("scalarShape", "map");

            DataStreamSource<ScalarEvent> left = env.addSource(
                    new ScalarHotKeySource(
                            0,
                            leftNumRecords,
                            hotKeyCount,
                            coldKeyCount,
                            hotTrafficPercent,
                            driftIntervalRecords,
                            driftStep,
                            timestampMode,
                            timestampHotBuckets,
                            timestampKeySpace,
                            timestampDriftIntervalRecords,
                            timestampDriftStep));

            DataStreamSource<ScalarEvent> right = env.addSource(
                    new ScalarHotKeySource(
                            rightDelayMs,
                            rightNumRecords,
                            hotKeyCount,
                            coldKeyCount,
                            hotTrafficPercent,
                            driftIntervalRecords,
                            driftStep,
                            timestampMode,
                            timestampHotBuckets,
                            timestampKeySpace,
                            timestampDriftIntervalRecords,
                            timestampDriftStep));

            SingleOutputStreamOperator<?> joined = left
                    .keyBy(ScalarEvent::joinKey)
                    .connect(right.keyBy(ScalarEvent::joinKey))
                    .process(new ScalarStateJoinFunction(scalarOpsPerRecord, mapKeyModulo, scalarShape))
                    .disableChaining();

            joined.addSink(new DiscardingSink<>()).disableChaining();
            env.execute("client-usecase-scalar-state-probe");
            return;
        }

        DataStreamSource<PVMVLogType> left = env.addSource(
                new DriftingHotKeySource(
                        0L,
                        0,
                        leftNumRecords,
                        hotKeyCount,
                        coldKeyCount,
                        hotTrafficPercent,
                        driftIntervalRecords,
                        driftStep,
                        payloadMode,
                        timestampMode,
                        timestampHotBuckets,
                        timestampKeySpace,
                        timestampDriftIntervalRecords,
                        timestampDriftStep));

        DataStreamSource<PVMVLogType> right = env.addSource(
                new DriftingHotKeySource(
                        20L,
                        rightDelayMs,
                        rightNumRecords,
                        hotKeyCount,
                        coldKeyCount,
                        hotTrafficPercent,
                        driftIntervalRecords,
                        driftStep,
                        payloadMode,
                        timestampMode,
                        timestampHotBuckets,
                        timestampKeySpace,
                        timestampDriftIntervalRecords,
                        timestampDriftStep));

        SingleOutputStreamOperator<?> joined = left
                .keyBy(PVMVLogType::joinKey)
                .connect(right.keyBy(PVMVLogType::joinKey))
                .process(new HuaweiTestFunction(Time.minutes(20), Time.minutes(20), false, 100))
                .disableChaining();

        joined.addSink(new DiscardingSink<>()).disableChaining();

        env.execute("client-usecase-hotspot-drift");
    }

    public static final class ScalarEvent {
        public long joinKey;
        public long eventTime;
        public long sequence;

        public ScalarEvent() {
        }

        ScalarEvent(long joinKey, long eventTime, long sequence) {
            this.joinKey = joinKey;
            this.eventTime = eventTime;
            this.sequence = sequence;
        }

        public Long joinKey() {
            return joinKey;
        }
    }

    private static final class ScalarStateJoinFunction
            extends KeyedCoProcessFunction<Long, ScalarEvent, ScalarEvent, Long> {
        private static final long serialVersionUID = 1L;

        private final int scalarOpsPerRecord;
        private final int mapKeyModulo;
        private final boolean valueOnly;
        private final boolean fusedMap;
        private final boolean batchMap;

        private transient ValueState<Long> leftCount;
        private transient ValueState<Long> rightCount;
        private transient MapState<Long, Long> leftBuckets;
        private transient MapState<Long, Long> rightBuckets;
        private transient Object leftForL0Buckets;
        private transient Object rightForL0Buckets;
        private transient Method leftAddAndGetLong;
        private transient Method rightAddAndGetLong;
        private transient Method leftAddSequentialAndSumLong;
        private transient Method rightAddSequentialAndSumLong;
        private transient Method leftSumSequentialLong;
        private transient Method rightSumSequentialLong;

        ScalarStateJoinFunction(int scalarOpsPerRecord, int mapKeyModulo, String scalarShape) {
            this.scalarOpsPerRecord = Math.max(1, scalarOpsPerRecord);
            this.mapKeyModulo = Math.max(1, mapKeyModulo);
            this.valueOnly = "value_only".equalsIgnoreCase(scalarShape);
            this.fusedMap = "map_fused".equalsIgnoreCase(scalarShape);
            this.batchMap = "map_batch".equalsIgnoreCase(scalarShape);
        }

        @Override
        public void open(Configuration parameters) {
            leftCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("ScalarLeftCount", Long.class));
            rightCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("ScalarRightCount", Long.class));
            leftBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("ScalarLeftBuckets", Long.class, Long.class));
            rightBuckets = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("ScalarRightBuckets", Long.class, Long.class));
            if (fusedMap || batchMap) {
                leftForL0Buckets = asForL0MapState(leftBuckets);
                rightForL0Buckets = asForL0MapState(rightBuckets);
                leftAddAndGetLong = resolveAddAndGetLong(leftForL0Buckets);
                rightAddAndGetLong = resolveAddAndGetLong(rightForL0Buckets);
                leftAddSequentialAndSumLong = resolveAddSequentialAndSumLong(leftForL0Buckets);
                rightAddSequentialAndSumLong = resolveAddSequentialAndSumLong(rightForL0Buckets);
                leftSumSequentialLong = resolveSumSequentialLong(leftForL0Buckets);
                rightSumSequentialLong = resolveSumSequentialLong(rightForL0Buckets);
                System.err.println("[ScalarState] scalar fast path: shape="
                        + (batchMap ? "map_batch" : "map_fused")
                        + ", leftStateClass=" + leftBuckets.getClass().getName()
                        + ", rightStateClass=" + rightBuckets.getClass().getName()
                        + ", leftForL0=" + (leftForL0Buckets != null)
                        + ", rightForL0=" + (rightForL0Buckets != null));
            }
        }

        @Override
        public void processElement1(ScalarEvent value, Context ctx, Collector<Long> out) throws Exception {
            long count = valueOrZero(leftCount.value()) + 1L;
            long checksum = count;
            if (valueOnly) {
                long local = count;
                for (int i = 0; i < scalarOpsPerRecord; i++) {
                    local += (value.eventTime + i) & 7L;
                    leftCount.update(local);
                    Long current = leftCount.value();
                    checksum += valueOrZero(current);
                }
                Long right = rightCount.value();
                if (right != null) {
                    checksum += right;
                }
                out.collect(checksum);
                return;
            }
            long base = value.eventTime;
            if (batchMap && leftAddSequentialAndSumLong != null) {
                Long batchSum = addSequentialAndSum(
                        leftForL0Buckets, leftAddSequentialAndSumLong, base, scalarOpsPerRecord, mapKeyModulo, 1L);
                if (batchSum != null) {
                    checksum += batchSum;
                    Long right = rightCount.value();
                    if (right != null) {
                        checksum += right;
                    }
                    leftCount.update(count);
                    out.collect(checksum);
                    return;
                }
            }
            for (int i = 0; i < scalarOpsPerRecord; i++) {
                long bucket = positiveMod(base + i, mapKeyModulo);
                long next = addAndGet(leftBuckets, leftForL0Buckets, leftAddAndGetLong, bucket, 1L);
                checksum += next;
            }
            Long right = rightCount.value();
            if (right != null) {
                checksum += right;
            }
            leftCount.update(count);
            out.collect(checksum);
        }

        @Override
        public void processElement2(ScalarEvent value, Context ctx, Collector<Long> out) throws Exception {
            long count = valueOrZero(rightCount.value()) + 1L;
            long checksum = count;
            if (valueOnly) {
                long local = count;
                for (int i = 0; i < scalarOpsPerRecord; i++) {
                    local += (value.eventTime + i) & 7L;
                    rightCount.update(local);
                    Long current = rightCount.value();
                    checksum += valueOrZero(current);
                    Long left = leftCount.value();
                    if (left != null) {
                        checksum += left;
                    }
                }
                out.collect(checksum);
                return;
            }
            long base = value.eventTime;
            if (batchMap && rightAddSequentialAndSumLong != null) {
                Long batchSum = addSequentialAndSum(
                        rightForL0Buckets, rightAddSequentialAndSumLong, base, scalarOpsPerRecord, mapKeyModulo, 1L);
                if (batchSum != null) {
                    checksum += batchSum;
                    Long leftSum = sumSequential(leftForL0Buckets, leftSumSequentialLong, base, scalarOpsPerRecord, mapKeyModulo);
                    if (leftSum != null) {
                        checksum += leftSum;
                    } else {
                        for (int i = 0; i < scalarOpsPerRecord; i++) {
                            long bucket = positiveMod(base + i, mapKeyModulo);
                            Long left = leftBuckets.get(bucket);
                            if (left != null) {
                                checksum += left;
                            }
                        }
                    }
                    rightCount.update(count);
                    out.collect(checksum);
                    return;
                }
            }
            for (int i = 0; i < scalarOpsPerRecord; i++) {
                long bucket = positiveMod(base + i, mapKeyModulo);
                long next = addAndGet(rightBuckets, rightForL0Buckets, rightAddAndGetLong, bucket, 1L);
                Long left = leftBuckets.get(bucket);
                if (left != null) {
                    checksum += left;
                }
                checksum += next;
            }
            rightCount.update(count);
            out.collect(checksum);
        }

        private static long valueOrZero(Long value) {
            return value == null ? 0L : value.longValue();
        }

        private static Object asForL0MapState(MapState<Long, Long> state) {
            if (isForL0MapState(state)) {
                return state;
            }
            try {
                Field originalState = state.getClass().getDeclaredField("originalState");
                originalState.setAccessible(true);
                Object unwrapped = originalState.get(state);
                return isForL0MapState(unwrapped) ? unwrapped : null;
            } catch (ReflectiveOperationException | RuntimeException e) {
                return null;
            }
        }

        private static boolean isForL0MapState(Object state) {
            return state != null
                    && "org.apache.flink.state.forl0.ForL0MapState".equals(state.getClass().getName());
        }

        private static long addAndGet(
                MapState<Long, Long> state,
                Object forl0State,
                Method addAndGetLong,
                long bucket,
                long delta) throws Exception {
            if (forl0State != null && addAndGetLong != null) {
                try {
                    return (Long) addAndGetLong.invoke(forl0State, bucket, delta);
                } catch (ReflectiveOperationException | RuntimeException ignored) {
                    // Historical stable ForL0 JARs do not expose the map fused helper.
                }
            }
            Long previous = state.get(bucket);
            long next = valueOrZero(previous) + delta;
            state.put(bucket, next);
            return next;
        }

        private static Long addSequentialAndSum(
                Object forl0State,
                Method addSequentialAndSumLong,
                long startUserKey,
                int count,
                long modulo,
                long delta) {
            if (forl0State == null || addSequentialAndSumLong == null) {
                return null;
            }
            try {
                return (Long) addSequentialAndSumLong.invoke(forl0State, startUserKey, count, modulo, delta);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }

        private static Long sumSequential(
                Object forl0State,
                Method sumSequentialLong,
                long startUserKey,
                int count,
                long modulo) {
            if (forl0State == null || sumSequentialLong == null) {
                return null;
            }
            try {
                return (Long) sumSequentialLong.invoke(forl0State, startUserKey, count, modulo);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }

        private static Method resolveAddAndGetLong(Object forl0State) {
            if (forl0State == null) {
                return null;
            }
            try {
                return forl0State.getClass().getMethod("addAndGetLong", Object.class, long.class);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }

        private static Method resolveAddSequentialAndSumLong(Object forl0State) {
            if (forl0State == null) {
                return null;
            }
            try {
                return forl0State.getClass().getMethod(
                        "addSequentialAndSumLong",
                        long.class,
                        int.class,
                        long.class,
                        long.class);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }

        private static Method resolveSumSequentialLong(Object forl0State) {
            if (forl0State == null) {
                return null;
            }
            try {
                return forl0State.getClass().getMethod(
                        "sumSequentialLong",
                        long.class,
                        int.class,
                        long.class);
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }
    }

    private static final class ScalarHotKeySource extends RichParallelSourceFunction<ScalarEvent> {
        private static final long serialVersionUID = 1L;

        private final int perRecordDelayMs;
        private final long maxRecords;
        private final int hotKeyCount;
        private final int coldKeyCount;
        private final int hotTrafficPercent;
        private final long driftIntervalRecords;
        private final int driftStep;
        private final String timestampMode;
        private final int timestampHotBuckets;
        private final int timestampKeySpace;
        private final long timestampDriftIntervalRecords;
        private final int timestampDriftStep;

        private volatile boolean running = true;

        ScalarHotKeySource(
                int perRecordDelayMs,
                long maxRecords,
                int hotKeyCount,
                int coldKeyCount,
                int hotTrafficPercent,
                long driftIntervalRecords,
                int driftStep,
                String timestampMode,
                int timestampHotBuckets,
                int timestampKeySpace,
                long timestampDriftIntervalRecords,
                int timestampDriftStep) {
            this.perRecordDelayMs = perRecordDelayMs;
            this.maxRecords = maxRecords;
            this.hotKeyCount = Math.max(1, hotKeyCount);
            this.coldKeyCount = Math.max(this.hotKeyCount, coldKeyCount);
            this.hotTrafficPercent = Math.max(0, Math.min(100, hotTrafficPercent));
            this.driftIntervalRecords = Math.max(1L, driftIntervalRecords);
            this.driftStep = Math.max(1, driftStep);
            this.timestampMode = timestampMode == null ? "monotonic" : timestampMode;
            this.timestampHotBuckets = Math.max(0, timestampHotBuckets);
            this.timestampKeySpace = Math.max(1, timestampKeySpace);
            this.timestampDriftIntervalRecords = Math.max(1L, timestampDriftIntervalRecords);
            this.timestampDriftStep = Math.max(1, timestampDriftStep);
        }

        @Override
        public void run(SourceContext<ScalarEvent> ctx) throws Exception {
            if (maxRecords == 0L) {
                return;
            }
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            long emitted = 0L;
            while (running && (maxRecords <= 0L || emitted < maxRecords)) {
                long globalIndex = emitted + ((long) subtask * 1_000_000_000L);
                long joinKey = chooseKey(globalIndex);
                long eventTime = chooseEventTime(globalIndex);
                ScalarEvent event = new ScalarEvent(joinKey, eventTime, globalIndex);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                    emitted++;
                }
                if (perRecordDelayMs > 0) {
                    Thread.sleep(perRecordDelayMs);
                }
            }
        }

        private int chooseKey(long index) {
            long phase = index / driftIntervalRecords;
            int hotBase = (int) ((phase * driftStep) % coldKeyCount);
            long mixed = mix64(index);
            int bucket = positiveMod(mixed, 100);
            if (bucket < hotTrafficPercent) {
                int offset = positiveMod(mix64(index ^ 0x9E3779B97F4A7C15L), hotKeyCount);
                return positiveMod((long) hotBase + offset, coldKeyCount);
            }
            return positiveMod(mix64(index ^ 0xD1B54A32D192ED03L), coldKeyCount);
        }

        private long chooseEventTime(long index) {
            if (!"hot_bucket".equalsIgnoreCase(timestampMode) || timestampHotBuckets <= 0) {
                return index;
            }
            long phase = index / timestampDriftIntervalRecords;
            int base = (int) ((phase * timestampDriftStep) % timestampKeySpace);
            int offset = positiveMod(mix64(index ^ 0x94D049BB133111EBL), timestampHotBuckets);
            return positiveMod((long) base + offset, timestampKeySpace);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class DriftingHotKeySource extends RichParallelSourceFunction<PVMVLogType> {
        private static final long serialVersionUID = 1L;

        private final long eventOffset;
        private final int perRecordDelayMs;
        private final long maxRecords;
        private final int hotKeyCount;
        private final int coldKeyCount;
        private final int hotTrafficPercent;
        private final long driftIntervalRecords;
        private final int driftStep;
        private final String payloadMode;
        private final String timestampMode;
        private final int timestampHotBuckets;
        private final int timestampKeySpace;
        private final long timestampDriftIntervalRecords;
        private final int timestampDriftStep;

        private volatile boolean running = true;

        private DriftingHotKeySource(
                long eventOffset,
                int perRecordDelayMs,
                long maxRecords,
                int hotKeyCount,
                int coldKeyCount,
                int hotTrafficPercent,
                long driftIntervalRecords,
                int driftStep,
                String payloadMode,
                String timestampMode,
                int timestampHotBuckets,
                int timestampKeySpace,
                long timestampDriftIntervalRecords,
                int timestampDriftStep) {
            this.eventOffset = eventOffset;
            this.perRecordDelayMs = perRecordDelayMs;
            this.maxRecords = maxRecords;
            this.hotKeyCount = Math.max(1, hotKeyCount);
            this.coldKeyCount = Math.max(this.hotKeyCount, coldKeyCount);
            this.hotTrafficPercent = Math.max(0, Math.min(100, hotTrafficPercent));
            this.driftIntervalRecords = Math.max(1L, driftIntervalRecords);
            this.driftStep = Math.max(1, driftStep);
            this.payloadMode = payloadMode == null ? "realistic" : payloadMode;
            this.timestampMode = timestampMode == null ? "monotonic" : timestampMode;
            this.timestampHotBuckets = Math.max(0, timestampHotBuckets);
            this.timestampKeySpace = Math.max(1, timestampKeySpace);
            this.timestampDriftIntervalRecords = Math.max(1L, timestampDriftIntervalRecords);
            this.timestampDriftStep = Math.max(1, timestampDriftStep);
        }

        @Override
        public void run(SourceContext<PVMVLogType> ctx) throws Exception {
            if (maxRecords == 0L) {
                return;
            }
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            long emitted = 0L;
            while (running && (maxRecords <= 0L || emitted < maxRecords)) {
                long globalIndex = emitted + ((long) subtask * 1_000_000_000L);
                int keyId = chooseKey(globalIndex);
                long eventTime = chooseEventTime(globalIndex) + eventOffset;
                PVMVLogType event = createEvent(globalIndex, eventTime);
                applyJoinKey(event, keyId);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                    emitted++;
                }
                if (perRecordDelayMs > 0) {
                    Thread.sleep(perRecordDelayMs);
                }
            }
        }

        private PVMVLogType createEvent(long globalIndex, long eventTime) throws IllegalAccessException {
            PVMVLogType event;
            if ("lightweight".equalsIgnoreCase(payloadMode)) {
                event = new PVMVLogType();
                event.setEventTimeStamp(eventTime);
                event.setSequenceKey(Long.valueOf((eventOffset << 48) ^ globalIndex));
                event.setLogType("lightweight");
                event.setLog("");
            } else {
                event = HuaweiMT6000c.createPVMV(globalIndex + eventOffset);
                event.setEventTimeStamp(eventTime);
            }
            return event;
        }

        private int chooseKey(long index) {
            long phase = index / driftIntervalRecords;
            int hotBase = (int) ((phase * driftStep) % coldKeyCount);
            long mixed = mix64(index);
            int bucket = positiveMod(mixed, 100);
            if (bucket < hotTrafficPercent) {
                int offset = positiveMod(mix64(index ^ 0x9E3779B97F4A7C15L), hotKeyCount);
                return positiveMod((long) hotBase + offset, coldKeyCount);
            }
            return positiveMod(mix64(index ^ 0xD1B54A32D192ED03L), coldKeyCount);
        }

        private long chooseEventTime(long index) {
            if (!"hot_bucket".equalsIgnoreCase(timestampMode) || timestampHotBuckets <= 0) {
                return index;
            }
            long phase = index / timestampDriftIntervalRecords;
            int base = (int) ((phase * timestampDriftStep) % timestampKeySpace);
            int offset = positiveMod(mix64(index ^ 0x94D049BB133111EBL), timestampHotBuckets);
            return positiveMod((long) base + offset, timestampKeySpace);
        }

        private static void applyJoinKey(PVMVLogType event, int keyId) {
            String key = Integer.toString(keyId);
            event.setPartitionLogChannel("channel" + key);
            event.setPartitionApp("app" + key);
            event.setSessionKey("session" + key);
            event.setRequestKey("request" + key);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
