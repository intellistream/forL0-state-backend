package org.apache.flink.benchmark.clientdrift;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.example.HuaweiMT6000c;
import org.example.HuaweiTestFunction;
import org.example.PVMVLogType;

/**
 * Non-contract client-usecase variant that keeps the customer's join/state logic but
 * replaces fixed CSV replay with a drifting-hot-key source.
 */
public final class ClientHotspotDriftBenchmark {

    private ClientHotspotDriftBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
            int bucket = (int) Math.floorMod(mixed, 100);
            if (bucket < hotTrafficPercent) {
                int offset = (int) Math.floorMod(mix64(index ^ 0x9E3779B97F4A7C15L), hotKeyCount);
                return Math.floorMod(hotBase + offset, coldKeyCount);
            }
            return (int) Math.floorMod(mix64(index ^ 0xD1B54A32D192ED03L), coldKeyCount);
        }

        private long chooseEventTime(long index) {
            if (!"hot_bucket".equalsIgnoreCase(timestampMode) || timestampHotBuckets <= 0) {
                return index;
            }
            long phase = index / timestampDriftIntervalRecords;
            int base = (int) ((phase * timestampDriftStep) % timestampKeySpace);
            int offset = (int) Math.floorMod(mix64(index ^ 0x94D049BB133111EBL), timestampHotBuckets);
            return Math.floorMod(base + offset, timestampKeySpace);
        }

        private static void applyJoinKey(PVMVLogType event, int keyId) {
            String key = Integer.toString(keyId);
            event.setPartitionLogChannel("channel" + key);
            event.setPartitionApp("app" + key);
            event.setSessionKey("session" + key);
            event.setRequestKey("request" + key);
        }

        private static long mix64(long z) {
            z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
            z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
            return z ^ (z >>> 33);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
